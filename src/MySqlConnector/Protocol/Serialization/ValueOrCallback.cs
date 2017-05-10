using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace MySql.Data.Protocol.Serialization
{
	internal sealed class ValueOrCallbackSource<TResult>
	{
		public ValueOrCallback<TResult> ValueOrCallback => Serialization.ValueOrCallback.FromSource(this);

		public void SetResult(TResult result)
		{
			m_result = result;
			(m_continuation ?? Interlocked.CompareExchange(ref m_continuation, s_sentinel, null))?.Invoke();
		}

		public void SetException(Exception exception)
		{
			m_exception = exception;
			(m_continuation ?? Interlocked.CompareExchange(ref m_continuation, s_sentinel, null))?.Invoke();
		}

		public void SetCompleted(Action continuation)
		{
			if (object.ReferenceEquals(m_continuation, s_sentinel) || object.ReferenceEquals(Interlocked.CompareExchange(ref m_continuation, continuation, null), s_sentinel))
				continuation();
			else if (m_continuation != continuation)
				throw new InvalidOperationException();
		}

		public ValueOrCallback<TNewResult> Then<TNewResult>(Func<TResult, ValueOrCallback<TNewResult>> onSuccess, Func<Exception, ValueOrCallback<TNewResult>> onFailure)
		{
			var newSource = new ValueOrCallbackSource<TNewResult>();
			if (IsCompleted)
				ThenContinuation(newSource, onSuccess, onFailure);
			else
				AddContinuation(newSource, onSuccess, onFailure);
			return newSource.ValueOrCallback;
		}

		private void AddContinuation<TNewResult>(ValueOrCallbackSource<TNewResult> otherSource, Func<TResult, ValueOrCallback<TNewResult>> onSuccess, Func<Exception, ValueOrCallback<TNewResult>> onFailure) =>
			SetCompleted(() => ThenContinuation(otherSource, onSuccess, onFailure));

		private void ThenContinuation<TNewResult>(ValueOrCallbackSource<TNewResult> otherSource, Func<TResult, ValueOrCallback<TNewResult>> onSuccess, Func<Exception, ValueOrCallback<TNewResult>> onFailure)
		{
			var newResult = m_exception != null ? onFailure(m_exception) : onSuccess(m_result);
			newResult.TransferTo(otherSource);
		}

		internal void Adopt(ValueOrCallbackSource<TResult> other)
		{
			if (other.m_exception != null)
				SetException(other.m_exception);
			else
				SetResult(other.m_result);
		}

		public bool IsCompleted => object.ReferenceEquals(m_continuation, s_sentinel);
		public bool IsCompletedSuccessfully => IsCompleted && m_exception == null;

		public TResult Result
		{
			get
			{
				if (m_exception != null)
					throw m_exception;
				return m_result;
			}
		}

		static readonly Action s_sentinel = () => { };

		Action m_continuation;
		TResult m_result;
		Exception m_exception;
	}

	internal static class ValueOrCallback
	{
		public static ValueOrCallback<TResult> FromResult<TResult>(TResult result) => new ValueOrCallback<TResult>(result);
		public static ValueOrCallback<TResult> FromException<TResult>(Exception exception) => new ValueOrCallback<TResult>(exception);
		public static ValueOrCallback<TResult> FromSource<TResult>(ValueOrCallbackSource<TResult> source) => new ValueOrCallback<TResult>(source);
	}
	
	internal struct ValueOrCallback<TResult> : INotifyCompletion
	{
	    internal ValueOrCallback(TResult result)
	    {
		    m_result = result;
		    m_source = null;
	    }

		internal ValueOrCallback(Exception exception)
		{
			m_result = default(TResult);
			m_source = new ValueOrCallbackSource<TResult>();
			m_source.SetException(exception);
		}

		internal ValueOrCallback(ValueOrCallbackSource<TResult> source)
		{
			m_result = default(TResult);
			m_source = source;
		}

		public ValueOrCallback<TResult> ConfigureAwait(bool configureAwait) => this;

		public ValueOrCallback<TResult> GetAwaiter() => this;

		public bool IsCompleted => m_source == null || m_source.IsCompleted;

		public bool IsCompletedSuccessfully => m_source == null || m_source.IsCompletedSuccessfully;

		public ValueOrCallback<TNewResult> Then<TNewResult>(Func<TResult, ValueOrCallback<TNewResult>> onSuccess) => Then(onSuccess, ValueOrCallback<TNewResult>.DefaultOnFailure);

		public ValueOrCallback<TNewResult> Then<TNewResult>(Func<TResult, ValueOrCallback<TNewResult>> onSuccess, Func<Exception, ValueOrCallback<TNewResult>> onFailure)
		{
			if (m_source == null)
				return onSuccess(m_result);
			return m_source.Then(onSuccess, onFailure);
		}

		public TResult Result => m_source != null ? m_source.Result : m_result;

		public static readonly ValueOrCallback<TResult> Empty = new ValueOrCallback<TResult>(default(TResult));

		public static readonly Func<Exception, ValueOrCallback<TResult>> DefaultOnFailure = ValueOrCallback.FromException<TResult>;

		public TResult GetResult() => Result;

		public void OnCompleted(Action continuation)
		{
			m_source.SetCompleted(continuation);
		}

		internal void TransferTo(ValueOrCallbackSource<TResult> target)
		{
			if (IsCompletedSuccessfully)
			{
				target.SetResult(Result);
			}
			else
			{
				var source = m_source;
				m_source.SetCompleted(() => target.Adopt(source));
			}
		}

		readonly TResult m_result;
		readonly ValueOrCallbackSource<TResult> m_source;
	}
}
