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

namespace FoundationDB.Linq
{
	using FoundationDB.Async;
	using FoundationDB.Client.Utils;
	using JetBrains.Annotations;
	using System;

	/// <summary>Wraps an async sequence of items into another async sequence of items</summary>
	/// <typeparam name="TSource">Type of elements of the inner async sequence</typeparam>
	/// <typeparam name="TResult">Type of elements of the outer async sequence</typeparam>
	internal sealed class FdbAsyncSequence<TSource, TResult> : IFdbAsyncEnumerable<TResult>
	{
		public readonly IFdbAsyncEnumerable<TSource> Source;
		public readonly Func<IFdbAsyncEnumerator<TSource>, IFdbAsyncEnumerator<TResult>> Factory;

		public FdbAsyncSequence([NotNull] IFdbAsyncEnumerable<TSource> source, [NotNull] Func<IFdbAsyncEnumerator<TSource>, IFdbAsyncEnumerator<TResult>> factory)
		{
			Contract.Requires(source != null && factory != null);
			this.Source = source;
			this.Factory = factory;
		}

		public IAsyncEnumerator<TResult> GetEnumerator()
		{
			return this.GetEnumerator(FdbAsyncMode.Default);
		}

		public IFdbAsyncEnumerator<TResult> GetEnumerator(FdbAsyncMode mode)
		{
			IFdbAsyncEnumerator<TSource> inner = null;
			try
			{
				inner = this.Source.GetEnumerator(mode);
				Contract.Requires(inner != null, "The underlying async sequence returned an empty enumerator");

				var outer = this.Factory(inner);
				if (outer == null) throw new InvalidOperationException("The async factory returned en empty enumerator");

				return outer;
			}
			catch (Exception)
			{
				//make sure that the inner iterator gets disposed if something went wrong
				if (inner != null) inner.Dispose();
				throw;
			}
		}
	}
}
