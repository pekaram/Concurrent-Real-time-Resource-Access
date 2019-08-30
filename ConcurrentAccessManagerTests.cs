using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace ServerUnitTest
{
    [TestClass]
    public class ConcurrentAccessManagerTests 
    {
        private readonly ManualResetEvent manualResetEvent = new ManualResetEvent(false);

        private ConcurrentAccessManager<Guid> concurrentAccessManager;

        /// <summary>
        /// Reference for invoking <see cref="TestCallBack(Guid)"/> as a callback.
        /// </summary>
        private ConcurrentAccessManagerTests referenceForInvokingCallback;
        
        private Guid anyIdAsTarget;

        private Guid lockId2;

        private Guid lockId;

        [TestInitialize]
        public void Initialize()
        {
            this.concurrentAccessManager = new ConcurrentAccessManager<Guid>();
            this.referenceForInvokingCallback = Substitute.For<ConcurrentAccessManagerTests>();

            anyIdAsTarget = Guid.NewGuid();
            lockId = Guid.NewGuid();
            lockId2 = Guid.NewGuid();
        }

        [TestMethod]
        public void AccessManagerOnParallelLocks()
        {
            List<Exception> exceptions = new List<Exception>();
            List<Thread> lockingThreads = new List<Thread>();

            for (int i = 0; i < 4; i++)
            {
                lockingThreads.Add(new Thread(() => LockResourceThread(lockId, exceptions)));
                lockingThreads[i].Start();
            }

            manualResetEvent.Set();

            foreach (var thread in lockingThreads)
            {
                thread.Join();
            }

            if (exceptions.Count != 0)
            {
                var errors = "";
                foreach(var ex in exceptions)
                {
                    errors += ex;
                }
                Assert.Fail(errors);
            }
        }

        private void LockResourceThread(Guid id, List<Exception> exceptions)
        {
            var guid = Guid.NewGuid();
            string name = Thread.CurrentThread.Name;
            
            manualResetEvent.WaitOne();

            try
            {
                concurrentAccessManager.TryLockResource(this.anyIdAsTarget, guid, null);
            }
            catch(Exception e)
            {
                exceptions.Add(e);
            }
        }

        [TestMethod]
        public void AccessManagerWhenLockedLocksEntity()
        {
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId);
            Assert.IsTrue(concurrentAccessManager.IsLocked(anyIdAsTarget));
        }

        [TestMethod]
        public void AccessManagerWhenLockedFailsToRelockEntity()
        {
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId, null);
            Assert.IsFalse(concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId2));
        }

        [TestMethod]
        public void AccessManagerUnlockedUnLocksEntity()
        {
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId, null);
            concurrentAccessManager.UnlockResource(anyIdAsTarget, lockId);
            Assert.IsFalse(concurrentAccessManager.IsLocked(anyIdAsTarget));
        }
       
        [TestMethod]
        public void AccessManagerWhenUnlockedInvokesPendingCallbacks()
        {
            var anyNumberOfLocks = 1000;

            List<Guid> lockIds = new List<Guid>();
            for(var i = 0; i < anyNumberOfLocks; i++)
            {
                lockIds.Add(Guid.NewGuid());
            }
            foreach (var lockId in lockIds)
            {
                concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId, referenceForInvokingCallback.TestCallBack);
            }
            foreach(var lockedId in lockIds)
            {
                concurrentAccessManager.UnlockResource(anyIdAsTarget, lockedId);
            }

            referenceForInvokingCallback.Received(anyNumberOfLocks - 1).TestCallBack(anyIdAsTarget);
        }

        [TestMethod]
        public void AccessManagerWhenLockedDoesNotInvokePendingCallback()
        {
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId);
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId2, referenceForInvokingCallback.TestCallBack);

            referenceForInvokingCallback.DidNotReceive().TestCallBack(anyIdAsTarget);
        }
        
        [TestMethod]
        public void AccessManagerWhenUnlockedDoesNotInovkeCancelledCallback()
        {
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId);
            concurrentAccessManager.TryLockResource(anyIdAsTarget, lockId2, referenceForInvokingCallback.TestCallBack);
            concurrentAccessManager.RemoveAllLocksForId(lockId2, true);
            concurrentAccessManager.UnlockResource(anyIdAsTarget, lockId);

            referenceForInvokingCallback.DidNotReceive().TestCallBack(anyIdAsTarget);
            Assert.IsTrue(true);
        }
      
        public virtual bool TestCallBack(Guid unlockedId)
        {
            return true;
        }       
    }
}
