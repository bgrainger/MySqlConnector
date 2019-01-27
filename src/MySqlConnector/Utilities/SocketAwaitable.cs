using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace MySqlConnector.Utilities
{
	// See http://blogs.msdn.com/b/pfxteam/archive/2011/12/15/10248293.aspx
	internal sealed class SocketAwaitable : INotifyCompletion
	{
		public SocketAwaitable(SocketAsyncEventArgs eventArgs)
		{
			EventArgs = eventArgs ?? throw new ArgumentNullException(nameof(eventArgs));
			eventArgs.Completed += (s, e) => (m_continuation ?? Interlocked.CompareExchange(ref m_continuation, s_sentinel, null))?.Invoke();
		}

		public SocketAwaitable GetAwaiter() => this;
		public bool IsCompleted => WasCompleted;

		public void OnCompleted(Action continuation)
		{
			if (m_continuation == s_sentinel || Interlocked.CompareExchange(ref m_continuation, continuation, null) == s_sentinel)
				Task.Run(continuation);
		}

		public void GetResult()
		{
			if (EventArgs.SocketError != SocketError.Success)
				throw new SocketException((int) EventArgs.SocketError);
		}

		internal bool WasCompleted { get; set; }
		internal SocketAsyncEventArgs EventArgs { get; }

		internal void Reset()
		{
			WasCompleted = false;
			m_continuation = null;
		}

		static readonly Action s_sentinel = () => { };

		Action? m_continuation;
	}
}
