using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

/// <summary>
/// Handles accessing objects from multiple threads concurrently in a sequential manner.
/// </summary>
/// <typeparam name="T"> object type </typeparam>.
public class ConcurrentAccessManager<T>
{
    private struct Lock : IEquatable<Lock>
    {
        /// <summary>
        /// Lock id, must be unique
        /// </summary>
        public readonly Guid LockId;

        /// <summary>
        /// Object to be locked
        /// </summary>
        public readonly T TargetObject;

        /// <summary>
        /// Callback to invoke later on if the object was locked at the point of locking
        /// </summary>
        public readonly UnlockedResourceCallback Callback;

        public Lock(Guid lockId, T targetObject, UnlockedResourceCallback callback)
        {
            this.LockId = lockId;
            this.TargetObject = targetObject;
            this.Callback = callback;
        }

        public bool Equals(Lock other)
        {
            return this.GetHashCode() == other.GetHashCode();
        }        
    }

    /// <summary>
    /// When a thread somewhere is trying to lock/unlock object, we use this to make other threads that want the same object wait.
    /// </summary>
    private readonly ConcurrentDictionary<T, Guid> activeOperations = new ConcurrentDictionary<T, Guid>();

    /// <summary>
    /// Locks currently active and would block other locking.
    /// </summary>
    private readonly ConcurrentDictionary<T, Lock> activeLocks = new ConcurrentDictionary<T, Lock>();

    /// <summary>
    /// all locks to ids
    /// </summary>
    private readonly ConcurrentDictionary<Guid, HashSet<T>> idToActiveLocks = new ConcurrentDictionary<Guid, HashSet<T>>();

    /// <summary>
    /// Locks that got queued.
    /// </summary>
    private readonly ConcurrentDictionary<T, ConcurrentQueue<Lock>> pendingLocks = new ConcurrentDictionary<T, ConcurrentQueue<Lock>>();

    /// <summary>
    /// Lock GUID to locks
    /// </summary>
    private readonly ConcurrentDictionary<Guid, HashSet<Lock>> idToPendingLocks = new ConcurrentDictionary<Guid, HashSet<Lock>>();

    /// <summary>
    /// Object that should've been removed but was blocked due to other threads.
    /// </summary>
    private readonly ConcurrentDictionary<T, Guid> removedActiveLocks = new ConcurrentDictionary<T, Guid>();

    /// <summary>
    /// Used when pending lock for an id in cancelled, to prevent further locks in the queue from invoking.
    /// The purpose is to stay O(1), instead of O(n) and iterating objects.
    /// </summary>
    private readonly HashSet<Guid> deactivatedPendingLockIds = new HashSet<Guid>();
    
    /// <summary>
    /// The callback to invoke when a resource is unlocked.
    /// </summary>
    /// <param name="unlockedResource"> the object unlocked </param>
    /// <returns> true if that callback's invocation successeded </returns>
    public delegate bool UnlockedResourceCallback(T unlockedResource);
    
    /// <summary>
    /// Checks for active locks
    public bool IsLocked(T objectToCheck)
    {   
        return this.activeLocks.ContainsKey(objectToCheck);      
    }
    
    /// <summary>
    /// Tries to lock a resource if available, otherwise queues its request.
    /// </summary>
    /// <param name="onUnlockedCallback"> callback to to queue for execution when object is free</param>
    /// <returns></returns>
    public bool TryLockResource(T objectToLock, Guid lockId, UnlockedResourceCallback onUnlockedCallback = null)
    {
        var isAvailableForOperation = this.activeOperations.TryAdd(objectToLock, lockId);
        if (!isAvailableForOperation)
        {
            if (onUnlockedCallback == null)
            {
                return false;
            }

            this.EnqueuePendingLock(objectToLock, lockId, onUnlockedCallback);
            // wait for operations to finish
            return false;
        }

        var isLocked = TryLockInternal(objectToLock, lockId, onUnlockedCallback);
        this.EndInternalLockOperation(objectToLock, lockId);
        return isLocked;
    }

    private bool TryLockInternal(T objectToLock, Guid lockId, UnlockedResourceCallback onUnlockedCallback = null)
    {
        if (!this.IsLocked(objectToLock))
        {
            this.ActivateLock(objectToLock, lockId, onUnlockedCallback);
            return true;
        }

        if (this.activeLocks[objectToLock].LockId == lockId)
        {
            return true;
        }
      
        if (onUnlockedCallback != null)
        {
            this.EnqueuePendingLock(objectToLock, lockId, onUnlockedCallback);
        }

        return false;
    }

    /// <summary>
    /// Unlocks resource if currently no other thread is attempting to do any operations.
    /// </summary>
    public bool UnlockResource(T resource, Guid releasedLockId, bool forceUnlockOperation = false)
    {
        var isAvailableForOperation = this.activeOperations.TryAdd(resource, releasedLockId);
        // Force unlock will directly try to unlock anyway.
        if (!forceUnlockOperation && !isAvailableForOperation)
        {
            // Object wasn't available for unlock operation.
            this.removedActiveLocks.TryAdd(resource, releasedLockId);
            return false;
        }

        if (!this.IsLocked(resource))
        {
            this.EndInternalLockOperation(resource, releasedLockId);
            return true;
        }
        
        if (this.activeLocks[resource].LockId != releasedLockId)
        {
            this.EndInternalLockOperation(resource, releasedLockId);
            return false;
        }

        Lock removedLock;
        this.activeLocks.TryRemove(resource, out removedLock);
        this.removedActiveLocks.TryRemove(resource, out _);
        this.idToActiveLocks[removedLock.LockId].Remove(removedLock.TargetObject);

        this.EndInternalLockOperation(resource, releasedLockId);

        if (this.AnyPendingExections(resource))
        {
            this.ResumePendingExecution(resource);
        }
        return true;
    }

    /// <summary>
    /// Removes all lock for a certian id if any
    /// <param name="cancelPendingRequests"> True will cancel any pending locks for the same id </param>
    public void RemoveAllLocksForId(Guid id, bool cancelPendingRequests)
    {
        if (idToActiveLocks.ContainsKey(id))
        {
            foreach (var removedLock in new List<T>(this.idToActiveLocks[id]))
            {
                this.UnlockResource(removedLock, id);
            }

            if (this.idToActiveLocks.Count == 0)
            {
                this.idToActiveLocks.TryRemove(id, out _);
            }
        }
       

        if (cancelPendingRequests && this.idToPendingLocks.ContainsKey(id))
        {
            this.deactivatedPendingLockIds.Add(id);
        }
    }

    /// <summary>
    /// If resource has locks requests pending
    /// </summary>
    /// <param name="resource"></param>
    /// <returns></returns>
    public bool AnyPendingExections(T resource)
    {
        return this.pendingLocks.ContainsKey(resource);
    }

    /// <summary>
    /// Qequeues lock requests for object 
    /// </summary>
    /// <param name="releasedObject"></param>
    private void ResumePendingExecution(T releasedObject)
    {
        Lock pendingLock;
        this.pendingLocks[releasedObject].TryDequeue(out pendingLock);
        this.RemoveIfNoPendingExecutions(releasedObject);

        if (this.IsPendingLockIdCancelled(pendingLock))
        {
            // Dequeue next element.
            if (this.AnyPendingExections(releasedObject))
            {
                this.ResumePendingExecution(releasedObject);
            }

            return;
        }

        this.ActivateLock(pendingLock);
        this.ExecuteLockCallback(pendingLock);
    }
    
    /// <summary>
    /// Invokes <see cref="UnlockedResourceCallback"/> for queued lock
    /// </summary>
    private void ExecuteLockCallback(Lock @lock)
    {
        @lock.Callback.Invoke(@lock.TargetObject);
        this.idToPendingLocks[@lock.LockId].Remove(@lock);

        if (this.idToPendingLocks[@lock.LockId].Count == 0)
        {
            this.idToPendingLocks.TryRemove(@lock.LockId, out _);
        }
    }

    /// <summary>
    /// True is the pending lock was cancelled before invocation.
    /// </summary>
    private bool IsPendingLockIdCancelled(Lock pendingLock)
    {
        if (this.deactivatedPendingLockIds.Contains(pendingLock.LockId))
        {
            // Pending lock was already cancelled, won't execute
            return true;
        }

        return false;
    }

    /// <summary>
    /// Removes object with no pending locks from <see cref="pendingLocks"/>
    /// </summary>
    /// <param name="releasedObject"></param>
    private void RemoveIfNoPendingExecutions(T releasedObject)
    {
        if (this.pendingLocks[releasedObject].Count == 0)
        {
            this.pendingLocks.TryRemove(releasedObject, out _);
        }
    }

    /// <summary>
    /// Activates locks by adding it to locked objects data
    /// </summary>
    private void ActivateLock(T objectToLock, Guid lockId, UnlockedResourceCallback callback)
    {
        var newLock = new Lock(lockId, objectToLock, callback);
        this.ActivateLock(newLock);
    }

    private void ActivateLock(Lock @lock)
    {
        if (!this.idToActiveLocks.ContainsKey(@lock.LockId))
        {
            this.idToActiveLocks.TryAdd(@lock.LockId, new HashSet<T>());
        }

        this.idToActiveLocks[@lock.LockId].Add(@lock.TargetObject);
        this.deactivatedPendingLockIds.Remove(@lock.LockId);
        var isSuccess = this.activeLocks.TryAdd(@lock.TargetObject, @lock);
        if(!isSuccess)
        {
            throw new Exception("object locked already, this should never happen");
        }
    }

    /// <summary>
    /// Enqueues a lock for execution after a resource is avaiable for locking
    /// </summary>
    private void EnqueuePendingLock(T objectToLock, Guid lockId, UnlockedResourceCallback onUnlockedCallback = null)
    {
        var newLock = new Lock(lockId, objectToLock, onUnlockedCallback);
        if (idToPendingLocks.ContainsKey(lockId)
            && idToPendingLocks[lockId].Contains(newLock))
        {
            // Already requested
            return;
        }
        if (!pendingLocks.ContainsKey(objectToLock)) 
        {
            this.pendingLocks.TryAdd(objectToLock, new ConcurrentQueue<Lock>());
        }
        if (!idToPendingLocks.ContainsKey(lockId))
        {
            this.idToPendingLocks.TryAdd(lockId, new HashSet<Lock>());
        }

        this.pendingLocks[objectToLock].Enqueue(newLock);
        this.idToPendingLocks[lockId].Add(newLock);
    }

    /// <summary>
    /// Ends lock when thread was attempting any kind of operation on an object
    /// </summary>
    /// <param name="targetObject"></param>
    /// <param name="id"></param>
    private void EndInternalLockOperation(T targetObject, Guid id)
    {
        // If a delayed removal
        if (this.removedActiveLocks.ContainsKey(targetObject))
        {
            if (this.activeLocks[targetObject].LockId != this.removedActiveLocks[targetObject])
            {
                return;
            }

            // Switch lock to block other threads from stealing this after unlocking, then force unlockresource to bypass checking in unlock resource
            var removalId = this.removedActiveLocks[targetObject];
            this.activeOperations[targetObject] = removalId;
            // Starting a forced removal.
            this.UnlockResource(targetObject, removalId, true);           
        }
        else
        {
            this.activeOperations.TryRemove(targetObject, out _);
        }
    }
}