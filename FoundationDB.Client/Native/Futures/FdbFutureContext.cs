﻿#region BSD Licence
/* Copyright (c) 2013-2014, Doxense SAS
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
	* Redistributions of source code must retain the above copyright
	  notice, this list of conditions and the following disclaimer.
	* Redistributions in binary form must reproduce the above copyright
	  notice, this list of conditions and the following disclaimer in the
	  documentation and/or other materials provided with the distribution.
	* Neither the name of Doxense nor the
	  names of its contributors may be used to endorse or promote products
	  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#endregion

// enable this to capture the stacktrace of the ctor, when troubleshooting leaked transaction handles
//#define CAPTURE_STACKTRACES

using FoundationDB.Async;

namespace FoundationDB.Client.Native
{
	using FoundationDB.Client.Utils;
	using JetBrains.Annotations;
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Runtime.CompilerServices;
	using System.Threading;
	using System.Threading.Tasks;

	internal class FdbFutureContext : IDisposable
	{

		#region Private Constants...

		private const int FUTURE_COOKIE_SIZE = 32;

		private const int FUTURE_COOKIE_SHIFT = 0;

		private const ulong FUTURE_COOKIE_MASK = (1UL << FUTURE_COOKIE_SIZE) - 1;

		private const int CONTEXT_COOKIE_SIZE = 32;

		private const ulong CONTEXT_COOKIE_MASK = (1UL << CONTEXT_COOKIE_SIZE) - 1;

		private const int CONTEXT_COOKIE_SHIFT = FUTURE_COOKIE_SIZE;

		#endregion

		#region Static Stuff....

		/// <summary>Counter used to generate the cookie values for each unique context</summary>
		private static int s_globalCookieCounter;

		private static readonly Dictionary<uint, FdbFutureContext> s_contexts = new Dictionary<uint, FdbFutureContext>();

		private static IntPtr MakeCallbackCookie(uint contextId, uint futureId)
		{
			ulong cookie = (contextId & CONTEXT_COOKIE_MASK) << CONTEXT_COOKIE_SHIFT;
			cookie |= (futureId & FUTURE_COOKIE_MASK) << FUTURE_COOKIE_SHIFT;
			return new IntPtr((long)cookie);
		}

		private static uint GetContextIdFromCookie(IntPtr cookie)
		{
			return (uint) (((ulong) cookie.ToInt64() >> CONTEXT_COOKIE_SHIFT) & CONTEXT_COOKIE_MASK);
		}

		private static uint GetFutureIdFromCookie(IntPtr cookie)
		{
			return (uint)(((ulong)cookie.ToInt64() >> FUTURE_COOKIE_SHIFT) & FUTURE_COOKIE_MASK);
		}

		/// <summary>Delegate that will be called by fdb_c to notify us that a future as completed</summary>
		/// <remarks>It is important to make sure that this delegate will NOT be garbaged collected before the last future callback has fired!</remarks>
		private static readonly FdbNative.FdbFutureCallback GlobalCallback = FutureCallbackHandler;

		private static void FutureCallbackHandler(IntPtr handle, IntPtr cookie)
		{
			// cookie is the value that will help us find the corresponding context (upper 32 bits) and future within this context (lower 32 bits) that matches with this future handle.

#if DEBUG_FUTURES
			Debug.WriteLine("FutureCallbackHandler(0x{0}, {1:X8} | {2:X8}) called from {3} [{4}]", handle.ToString("X"), cookie.ToInt64() >> 32, cookie.ToInt64() & uint.MaxValue, Thread.CurrentThread.ManagedThreadId, Thread.CurrentThread.Name);
#endif
			bool fromNetworkThread = Fdb.IsNetworkThread;

			if (!fromNetworkThread)
			{ // most probably, we have been called inline from fdb_future_set_callback
				// => The caller is holding a lock, so we have to defer to the ThreadPool and return as soon as possible!
				try
				{
					ThreadPool.UnsafeQueueUserWorkItem(
						(state) =>
						{
							var args = (Tuple<IntPtr, IntPtr>) state;
							ProcessFutureCallback(args.Item1, args.Item2, false);
						},
						Tuple.Create(handle, cookie)
					);
					return;
				}
				catch (Exception)
				{ // unable to defer to the TP?
					// we can't rethrow the exception if FDB_C is calling us (it won't know about it),
					// so we will continue running inline. Hopefully this should never happen.

					// => eat the exception and continue
				}
			}

			ProcessFutureCallback(handle, cookie, fromNetworkThread);
		}

		private static void ProcessFutureCallback(IntPtr handle, IntPtr cookie, bool fromNetworkThread)
		{
#if DEBUG_FUTURES
			Debug.WriteLine("ProcessFutureCallback(0x{0}, {1:X8} | {2:X8}, {3}) called from {4} [{5}]", handle.ToString("X"), cookie.ToInt64() >> 32, cookie.ToInt64() & uint.MaxValue, fromNetworkThread, Thread.CurrentThread.ManagedThreadId, Thread.CurrentThread.Name);
#endif
			// we are called by FDB_C, from the thread that runs the Event Loop
			bool keepAlive = false;
			try
			{
				// extract the upper 32 bits which contain the ID of the corresponding future context
				uint contextId = GetContextIdFromCookie(cookie);

				FdbFutureContext context;
				lock (s_contexts) // there will only be contentions on this lock if other a lot of threads are creating new contexts (ie: new transactions)
				{
					s_contexts.TryGetValue(contextId, out context);
				}

				if (context != null)
				{
					//TODO: if the context is marked as "dead" we need to refcount the pending futures down to 0, and then remove the context from the list

					Contract.Assert(context.m_contextId == contextId);
					bool purgeContext;
					keepAlive = context.OnFutureReady(handle, cookie, fromNetworkThread, out purgeContext);

					if (purgeContext)
					{ // the context was disposed and saw the last pending future going by, we have to remove it from the list
						lock (s_contexts)
						{
							s_contexts.Remove(contextId);
						}
					}
				}
			}
			finally
			{
				if (!keepAlive) DestroyHandle(ref handle);
			}
		}

		#endregion

		private const int STATE_DEFAULT = 0;

		private const int STATE_DEAD = 1;

		// this flag must only be used under the lock
		private int m_flags;

		/// <summary>Cookie for this context</summary>
		/// <remarks>Makes the 32-bits upper bits of the future callback parameter</remarks>
		private readonly uint m_contextId = (uint) Interlocked.Increment(ref s_globalCookieCounter);

		/// <summary>Counter used to generated the cookie for all futures created from this context</summary>
		private int m_localCookieCounter;

		/// <summary>Dictionary used to store all the pending Futures for this context</summary>
		/// <remarks>All methods should take a lock on this instance before manipulating the state</remarks>
		private readonly Dictionary<uint, IFdbFuture> m_futures = new Dictionary<uint, IFdbFuture>();

#if CAPTURE_STACKTRACES
		private readonly StackTrace m_stackTrace;
#endif

		#region Constructors...

		protected FdbFutureContext()
		{
			//REVIEW: is this a good idea to do this in the constructor? (we could start observing a context that hasn't been fully constructed yet
			lock (s_contexts)
			{
				s_contexts[m_contextId] = this;
			}
#if CAPTURE_STACKTRACES
			m_stackTrace = new StackTrace();
#endif
		}

#if NOT_NEEDED
		//REVIEW: do we really need a destructor ? The handle is a SafeHandle, and will take care of itself...
		~FdbFutureContext()
		{
			if (!AppDomain.CurrentDomain.IsFinalizingForUnload())
			{
#if CAPTURE_STACKTRACES
				Debug.WriteLine("A future context ({0}) was leaked by {1}", this, m_stackTrace);
#endif
#if DEBUG
				// If you break here, that means that a native transaction handler was leaked by a FdbTransaction instance (or that the transaction instance was leaked)
				if (Debugger.IsAttached) Debugger.Break();
#endif
				Dispose(false);
			}
		}
#endif

		#endregion

		#region IDisposable...

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				// 

#if DEBUG_FUTURES
				Debug.WriteLine("Disposing context {0}#{1} with {2} pending future(s) ({3} total)", this.GetType().Name, m_contextId, m_futures.Count, m_localCookieCounter);
#endif
				bool purge;
				lock (m_futures)
				{
					if (m_flags == STATE_DEAD)
					{ // already dead!
						return;
					}
					m_flags = STATE_DEAD;
					purge = m_futures.Count == 0;
				}

				if (purge)
				{ // no pending futures, we can remove ourselves from the global list
					lock (s_contexts)
					{
						s_contexts.Remove(m_contextId);
#if DEBUG_FUTURES
						Debug.WriteLine("Dumping all remaining contexts: {0}", s_contexts.Count);
						foreach (var ctx in s_contexts)
						{
							Debug.WriteLine("- {0}#{1} : {2} ({3})", ctx.Value.GetType().Name, ctx.Key, ctx.Value.m_futures.Count, ctx.Value.m_localCookieCounter);
						}
#endif
					}
				}
				//else: we have to wait for all callbacks to fire. The last one will remove this context from the global list
			}
		}

		#endregion

		/// <summary>A callback has fire for a future handled by this context</summary>
		private bool OnFutureReady(IntPtr handle, IntPtr cookie, bool fromNetworkThread, out bool purgeContext)
		{
			uint futureId = GetFutureIdFromCookie(cookie);

			purgeContext = false;
			IFdbFuture future;
			lock (m_futures)
			{
				if (m_flags == STATE_DEAD)
				{ // we are just waiting for all callbacks to fire
					m_futures.Remove(futureId);
					purgeContext = m_futures.Count == 0;
                    return false;
				}

				m_futures.TryGetValue(futureId, out future);
			}

			if (future != null && future.Cookie == cookie)
			{
				if (future.Visit(handle))
				{ // future is ready to process all the results

					lock (m_futures)
					{
						m_futures.Remove(futureId);
					}

					if (fromNetworkThread)
					{
						ThreadPool.UnsafeQueueUserWorkItem((state) => ((IFdbFuture)state).OnReady(), future);
						//TODO: what happens if TP.UQUWI() fails?
					}
					else
					{
						future.OnReady();
					}
				}
				// else: expecting more handles

				// handles will be destroyed when the future completes
				return true;
			}
			return false;
		}

		/// <summary>Add a new future handle to this context</summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="handle">Handle of the newly created future</param>
		/// <param name="mustDispose">Flag set to true if the future must be disposed by the caller (in case of error), or false if the future will be disposed by some other thread.</param>
		/// <param name="selector">Method called when the future completes successfully</param>
		/// <param name="state">State that will be passed as the second argument to <paramref name="selector"/></param>
		/// <param name="ct">TODO: remove this?</param>
		/// <param name="label">Type of future (name of the caller)</param>
		/// <returns></returns>
		protected Task<TResult> RegisterFuture<TResult>(
			IntPtr handle,
			ref bool mustDispose,
			[NotNull] Func<IntPtr, object, TResult> selector,
			object state,
			CancellationToken ct,
			string label
		)
		{
			if (ct.IsCancellationRequested) return TaskHelpers.FromCancellation<TResult>(ct);

			FdbFutureSingle<TResult> future = null;
			IntPtr cookie = IntPtr.Zero;
			uint futureId = (uint)Interlocked.Increment(ref m_localCookieCounter);

			try
			{
				cookie = MakeCallbackCookie(m_contextId, futureId);

				future = new FdbFutureSingle<TResult>(handle, selector, state, cookie, label);

				if (FdbNative.FutureIsReady(handle))
				{ // the result is already computed
#if DEBUG_FUTURES
					Debug.WriteLine("FutureSingle.{0} 0x{1} already completed!", label, handle.ToString("X"));
#endif
					cookie = IntPtr.Zero;
					mustDispose = false;
					future.OnReady();
					return future.Task;
				}

				if (ct.CanBeCanceled)
				{
					if (ct.IsCancellationRequested)
					{
						future.TrySetCanceled();
						cookie = IntPtr.Zero;
						return future.Task;
					}

					// note that the cancellation handler can fire inline, but it will only mark the future as cancelled
					// this means that we will still wait for the future callback to fire and set the task state in there.
					future.m_ctr = RegisterForCancellation(future, ct);
				}

				lock (m_futures)
				{
					m_futures[futureId] = future;

					// note: if the future just got ready, the callback will fire inline (as of v3.0)
					// => if this happens, the callback defer the execution to the ThreadPool and returns immediately
					var err = FdbNative.FutureSetCallback(handle, GlobalCallback, cookie);
					if (!Fdb.Success(err))
					{ // the callback will not fire, so we have to abort the future immediately
						future.PublishError(null, err);
					}
					else
					{
						mustDispose = false;
					}
				}
				return future.Task;
			}
			catch (Exception e)
			{
				if (future != null)
				{
					future.PublishError(e, FdbError.UnknownError);
					return future.Task;
				}
				throw;
			}
			finally
			{
				if (mustDispose && cookie != IntPtr.Zero)
				{ // make sure that we never leak a failed future !
					lock (m_futures)
					{
						m_futures.Remove(futureId);
					}
				}
			}
		}

		/// <summary>Add a new future handle to this context</summary>
		/// <typeparam name="TResult"></typeparam>
		/// <param name="handles">Handles of the newly created future</param>
		/// <param name="mustDispose">Flag set to true if the future must be disposed by the caller (in case of error), or false if the future will be disposed by some other thread.</param>
		/// <param name="selector">Method called when the future completes successfully</param>
		/// <param name="state">State that will be passed as the second argument to <paramref name="selector"/></param>
		/// <param name="ct">TODO: remove this?</param>
		/// <param name="label">Type of future (name of the caller)</param>
		/// <returns></returns>
		protected Task<TResult[]> RegisterFutures<TResult>(
			[NotNull] IntPtr[] handles,
			ref bool mustDispose,
			[NotNull] Func<IntPtr, object, TResult> selector,
			object state,
			CancellationToken ct,
			string label
		)
		{
			if (ct.IsCancellationRequested) return TaskHelpers.FromCancellation<TResult[]>(ct);

			FdbFutureArray<TResult> future = null;
			IntPtr cookie = IntPtr.Zero;
			uint futureId = (uint) Interlocked.Increment(ref m_localCookieCounter);
			try
			{
				cookie = MakeCallbackCookie(m_contextId, futureId);

				// make a copy because we may diverge from the caller if we partially fail to register the callbacks below
				var tmp = new IntPtr[handles.Length];
				handles.CopyTo(tmp, 0);
				future = new FdbFutureArray<TResult>(tmp, selector, state, cookie, label);

				// check the case where all futures are already ready (served from cache?)
				bool ready = true;
				foreach (var handle in tmp)
				{
					if (!FdbNative.FutureIsReady(handle))
					{
						ready = false;
						break;
					}
				}
				if (ready)
				{
#if DEBUG_FUTURES
					Debug.WriteLine("FutureArray.{0} [{1}] already completed!", label, tmp.Length);
#endif
					cookie = IntPtr.Zero;
					mustDispose = false;
					future.OnReady();
					return future.Task;
				}

				if (ct.CanBeCanceled)
				{
					future.m_ctr = RegisterForCancellation(future, ct);
					if (future.Task.IsCompleted)
					{ // cancellation ran inline
						future.TrySetCanceled();
						return future.Task;
					}
				}

				lock (m_futures)
				{
					m_futures[futureId] = future;

					// since the callbacks can fire inline, we have to make sure that we finish setting everything up under the lock
					for (int i = 0; i < handles.Length; i++)
					{
						FdbError err = FdbNative.FutureSetCallback(handles[i], GlobalCallback, cookie);
						if (Fdb.Success(err))
						{
							handles[i] = IntPtr.Zero;
							continue;
						}

						// we have to cleanup everything, and mute this future
						lock (m_futures)
						{
							m_futures.Remove(futureId);
							for (int j = i + 1; j < handles.Length; j++)
							{
								tmp[j] = IntPtr.Zero;
							}
						}

						throw Fdb.MapToException(err);
					}
				}
				mustDispose = false;
				return future.Task;
			}
			catch (Exception e)
			{
				if (future != null)
				{
					future.PublishError(e, FdbError.UnknownError);
					return future.Task;
				}
				throw;
			}
			finally
			{
				if (mustDispose && cookie != IntPtr.Zero)
				{ // make sure that we never leak a failed future !
					lock (m_futures)
					{
						m_futures.Remove(futureId);
					}
				}

			}
		}

		/// <summary>Start a new async operation</summary>
		/// <typeparam name="TResult">Result of the operation</typeparam>
		/// <param name="generator">Lambda called to produce the future handle</param>
		/// <param name="argument">Argument passed to <paramref name="generator"/>. It will not be used after the handle has been constructed</param>
		/// <param name="selector">Lambda called once the future completes (successfully)</param>
		/// <param name="state">State object passed to <paramref name="selector"/>. It will be stored in the future has long as it is active.</param>
		/// <param name="ct">Optional cancellation token used to cancel the task from an external source.</param>
		/// <param name="label">Optional label, used for logging and troubleshooting purpose (by default the name of the caller)</param>
		/// <returns></returns>
		protected Task<TResult> RunAsync<TResult>(
			[NotNull] Func<object, IntPtr> generator,
			object argument,
			[NotNull] Func<IntPtr, object, TResult> selector,
			object state,
			CancellationToken ct,
			[CallerMemberName] string label = null
		)
		{
			if (ct.IsCancellationRequested) return TaskHelpers.FromCancellation<TResult>(ct);

			bool mustDispose = true;
			IntPtr h = IntPtr.Zero;
			try
			{
				RuntimeHelpers.PrepareConstrainedRegions();
				try
				{ }
				finally
				{
					h = generator(argument);
				}

				return RegisterFuture(h, ref mustDispose, selector, state, ct, label);
			}
			finally
			{
				if (mustDispose && h != IntPtr.Zero)
				{
					FdbNative.FutureDestroy(h);
				}
			}
		}

		internal static CancellationTokenRegistration RegisterForCancellation(IFdbFuture future, CancellationToken cancellationToken)
		{
			//note: if the token is already cancelled, the callback handler will run inline and any exception would bubble up here
			//=> this is not a problem because the ctor already has a try/catch that will clean up everything
			return cancellationToken.RegisterWithoutEC(
				(_state) => { CancellationHandler(_state); },
				future
			);
		}

		private static void CancellationHandler(object state)
		{
			var future = (IFdbFuture)state;
			Contract.Assert(state != null);
#if DEBUG_FUTURES
			Debug.WriteLine("CancellationHandler for " + future + " was called on thread #" + Thread.CurrentThread.ManagedThreadId.ToString());
#endif
			future.Cancel();
		}

		internal static void DestroyHandle(ref IntPtr handle)
		{
			if (handle != IntPtr.Zero)
			{
				FdbNative.FutureDestroy(handle);
				handle = IntPtr.Zero;
			}
		}

		internal static void DestroyHandles(ref IntPtr[] handles)
		{
			if (handles != null)
			{
				foreach (var handle in handles)
				{
					if (handle != IntPtr.Zero) FdbNative.FutureDestroy(handle);
				}
				handles = null;
			}
		}

		internal const int CATEGORY_SUCCESS = 0;
		internal const int CATEGORY_RETRYABLE = 1;
		internal const int CATEGORY_CANCELLED = 2;
		internal const int CATEGORY_FAILURE = 3;

		internal static int ClassifyErrorSeverity(FdbError error)
		{
			switch (error)
			{
				case FdbError.Success:
				{
					return CATEGORY_SUCCESS;
				}
				case FdbError.PastVersion:
				case FdbError.FutureVersion:
				case FdbError.NotCommitted:
				case FdbError.CommitUnknownResult:
				{
					return CATEGORY_RETRYABLE;
				}

				case FdbError.OperationCancelled: // happens if a future is cancelled (probably a watch)
				case FdbError.TransactionCancelled: // happens if a transaction is cancelled (via its own parent CT, or via tr.Cancel())
				{
					return CATEGORY_CANCELLED;
				}

				default:
				{
					return CATEGORY_FAILURE;
				}
			}
		}
	}

	internal class FdbFutureContext<THandle> : FdbFutureContext
		where THandle : FdbSafeHandle
	{

		protected readonly THandle m_handle;

		protected FdbFutureContext([NotNull] THandle handle)
		{
			if (handle == null) throw new ArgumentNullException("handle");
			m_handle = handle;
		}

		public THandle Handle { [NotNull] get { return m_handle; } }

		protected override void Dispose(bool disposing)
		{
			try
			{
				base.Dispose(disposing);
			}
			finally
			{
				if (disposing)
				{
					lock (this.Handle)
					{
						if (!this.Handle.IsClosed) this.Handle.Dispose();
					}
				}
			}
		}

		/// <summary>Start a new async operation</summary>
		/// <typeparam name="TResult">Type of the result of the operation</typeparam>
		/// <typeparam name="TArg">Type of the argument passed to the generator</typeparam>
		/// <param name="generator">Lambda called to produce the future handle</param>
		/// <param name="argument">Argument passed to <paramref name="generator"/>. It will not be used after the handle has been constructed</param>
		/// <param name="selector">Lambda called once the future completes (successfully)</param>
		/// <param name="state">State object passed to <paramref name="selector"/>. It will be stored in the future has long as it is active.</param>
		/// <param name="ct">Optional cancellation token used to cancel the task from an external source.</param>
		/// <param name="label">Optional label, used for logging and troubleshooting purpose (by default the name of the caller)</param>
		/// <returns></returns>
		protected Task<TResult> RunAsync<TResult, TArg>(
			[NotNull] Func<THandle, TArg, IntPtr> generator,
			TArg argument,
			[NotNull] Func<IntPtr, object, TResult> selector,
			object state,
			CancellationToken ct,
			[CallerMemberName] string label = null
			)
		{
			if (ct.IsCancellationRequested) return TaskHelpers.FromCancellation<TResult>(ct);

			bool mustDispose = true;
			IntPtr h = IntPtr.Zero;
			try
			{
				lock (this.Handle)
				{
					if (this.Handle.IsClosed) throw new ObjectDisposedException(this.GetType().Name);
					h = generator(m_handle, argument);
				}
				return RegisterFuture<TResult>(h, ref mustDispose, selector, state, ct, label);
			}
			finally
			{
				if (mustDispose && h != IntPtr.Zero)
				{
					FdbNative.FutureDestroy(h);
				}
			}
		}

		protected Task<TResult[]> RunAsync<TResult, TArg>(
			int count,
			Action<THandle, TArg, IntPtr[]> generator,
			TArg arg,
			Func<IntPtr, object, TResult> selector,
			object state,
			CancellationToken ct,
			[CallerMemberName] string label = null

        )
		{
			bool mustDispose = true;
			var handles = new IntPtr[count];
			try
			{
				lock (this.Handle)
				{
					if (this.Handle.IsClosed) throw new ObjectDisposedException(this.GetType().Name);
					generator(m_handle, arg, handles);
				}
				return RegisterFutures<TResult>(handles, ref mustDispose, selector, state, ct, label);
			}
			catch
			{
				foreach (var future in handles)
				{
					if (future != IntPtr.Zero) FdbNative.FutureDestroy(future);
				}
				throw;
			}
		}

	}

}
