using System;
using System.Collections.Generic;
using System.Linq;
using System.Messaging;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LiteQ
{
	public static class Transit
	{
		private static readonly Dictionary<string, MessageQueue> _InBound = new Dictionary<string, MessageQueue>();
		private static readonly Dictionary<int, SenderInfo> _Outbound = new Dictionary<int, SenderInfo>();
		private static readonly Dictionary<string, MessageQueue> _Administration = new Dictionary<string, MessageQueue>();

		private static readonly IPAddress _LocalIP = GetLocalIP();
		
		public static Sender<T> Register<T>(string queueName, Func<T,T> localHandler)
		{
			return new Sender<T>(queueName, localHandler);
		}

		public static Sender<TRequest, TResponse> Regsiter<TRequest, TResponse>(string queueName, Func<TRequest, TResponse> localHandler )
		{
			return new Sender<TRequest, TResponse>(queueName, localHandler);
		}

		public abstract class SenderBase
		{
			protected readonly string _QueueName;

			protected MessageQueue _ResponseQueue;

			protected SenderBase(string queueName)
			{
				_QueueName = queueName;
				AddAdminQueue(queueName);
			}

			protected Task<TResponse> Send<TRequest, TResponse>(IPAddress peerIP, TRequest body, Dictionary<string, TaskCompletionSource<TResponse>> jobs)
			{
				//if done locally, just do it

				TaskCompletionSource<TResponse> taskCompletionSource = new TaskCompletionSource<TResponse>();

				try
				{
					int outboundKeyHash = (peerIP + _QueueName).GetHashCode();

					AddOutboundQueue(peerIP);

					string correlationId = Guid.NewGuid() + @"\" + "1";

					Message message = new Message
					{
						Body = body,
						AdministrationQueue = new MessageQueue { Path = _Outbound[outboundKeyHash].ResponseAdminPath },
						CorrelationId = correlationId,
						AcknowledgeType = AcknowledgeTypes.PositiveArrival | AcknowledgeTypes.PositiveReceive,
						ResponseQueue = new MessageQueue { Path = _Outbound[outboundKeyHash].ResponseQueuePath },
						Formatter = new BinaryMessageFormatter()
					};

					jobs.Add(correlationId, taskCompletionSource);

					using (var transaction = new MessageQueueTransaction())
					{
						transaction.Begin();
						_Outbound[outboundKeyHash].OutboundQueue.Send(message, transaction);
						transaction.Commit();
					}

					return taskCompletionSource.Task;
				}
				catch (Exception e)
				{
					return null;
				}
			}

			protected void AddOutboundQueue(IPAddress peerIP)
			{
				int outboundKeyHash = (peerIP + _QueueName).GetHashCode();

				if (!_Outbound.ContainsKey(outboundKeyHash))
				{
					string outboundQueuePath = String.Format(@"FormatName:Direct=TCP:{0}\Private$\{1}", peerIP, _QueueName);

					SenderInfo outboundInfo = new SenderInfo
					{
						OutboundQueuePath = outboundQueuePath,
						ResponseQueuePath = String.Format(@"FormatName:Direct=TCP:{0}\Private$\Response_{1}", _LocalIP, _QueueName),
						ResponseAdminPath = String.Format(@"FormatName:Direct=TCP:{0}\Private$\Administration_{1}", _LocalIP, _QueueName),
						OutboundQueue = new MessageQueue
						{
							Path = outboundQueuePath,
							Formatter = new BinaryMessageFormatter(),
						}
					};

					_Outbound.Add(outboundKeyHash, outboundInfo);
				}
			}
			
			protected void AddAdminQueue(string queueName)
			{
				string administrationQueuePath = String.Format(@".\Private$\Administration_{0}", queueName);

				if (!MessageQueue.Exists(administrationQueuePath))
					MessageQueue.Create(administrationQueuePath);

				_Administration.Add(queueName, new MessageQueue
				{
					Path = administrationQueuePath,
					Formatter = new BinaryMessageFormatter()
				});
			}

			protected void AddResponseQueue<T>(string queueName, Dictionary<string, TaskCompletionSource<T>> jobs)
			{
				string responsePath = String.Format(@".\Private$\Response_{0}", queueName);

				if (!MessageQueue.Exists(responsePath))
					MessageQueue.Create(responsePath, true);

				_ResponseQueue = new MessageQueue
				{
					Path = responsePath,
					Formatter = new BinaryMessageFormatter(),
					MessageReadPropertyFilter = new MessagePropertyFilter
					{
						Body = true,
						CorrelationId = true,
					},
				};

				_ResponseQueue.ReceiveCompleted += (s, args) => ResponseListener(s, args, jobs);
				_ResponseQueue.BeginReceive(MessageQueue.InfiniteTimeout);
			}

			protected void AddInboundQueue<TRequest, TResponse>(string queueName, Func<TRequest, TResponse> localHandler)
			{
				if (_InBound.ContainsKey(queueName))
					throw new ArgumentException("Queue with that name already exits");

				string queuePath = String.Format(@".\Private$\{0}", queueName);

				if (!MessageQueue.Exists(queuePath))
					MessageQueue.Create(queuePath, true);


				var inboundQ = new MessageQueue
				{
					Path = queuePath,
					Formatter = new BinaryMessageFormatter(),
					MessageReadPropertyFilter = new MessagePropertyFilter
					{
						Body = true,
						CorrelationId = true,
						ResponseQueue = true,
						AdministrationQueue = true,
						AcknowledgeType = true
					},
				};

				inboundQ.ReceiveCompleted += (s, args) => ListenReceiver(s, args, localHandler);
				inboundQ.BeginReceive(MessageQueue.InfiniteTimeout);

				_InBound.Add(queueName, inboundQ);
			}

			protected void ListenReceiver<TRequest, TResponse>(object sender, ReceiveCompletedEventArgs receiveCompletedEventArgs, Func<TRequest, TResponse> localHandler )
			{
				Message message = null;

				MessageQueue mq = (MessageQueue)sender;

				try
				{
					message = mq.EndReceive(receiveCompletedEventArgs.AsyncResult);
				}
				catch (MessageQueueException ex) { }

				mq.BeginReceive(MessageQueue.InfiniteTimeout);

				try
				{
					if (message != null)
					{
						TRequest item = (TRequest)message.Body;

						Message returnMessage = new Message
						{
							Body = localHandler(item),
							Formatter = new BinaryMessageFormatter(),
							CorrelationId = message.CorrelationId
						};

						using (var transaction = new MessageQueueTransaction())
						{
							transaction.Begin();
							message.ResponseQueue.Send(returnMessage, transaction);
							transaction.Commit();
						}
					}
					else
					{
						Console.WriteLine("Message was null");
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
				}
			}

			protected void ResponseListener<T>(object sender, ReceiveCompletedEventArgs receiveCompletedEventArgs, Dictionary<string, TaskCompletionSource<T>> jobs)
			{
				MessageQueue mq = (MessageQueue)sender;

				try
				{
					Message message = mq.EndReceive(receiveCompletedEventArgs.AsyncResult);

					if (message != null)
					{
						T item = (T)message.Body;
						jobs[message.CorrelationId].SetResult(item);
						Console.WriteLine(item.GetType());
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex);
				}

				mq.BeginReceive(MessageQueue.InfiniteTimeout);
			}
		}

		public class Sender<T> : SenderBase
		{
			private readonly Dictionary<string, TaskCompletionSource<T>> _Jobs =
				new Dictionary<string, TaskCompletionSource<T>>();

			public Sender(string queueName, Func<T,T> localHandler ) : base(queueName)
			{
				AddInboundQueue(queueName, localHandler);
				AddResponseQueue(_QueueName, _Jobs);
			}

			public Task<T> Send(IPAddress peerIP, T body)
			{
				return Send<T, T>(peerIP, body, _Jobs);
			}
		}

		public class Sender<TRequest, TResponse> : SenderBase
		{
			private readonly Dictionary<string, TaskCompletionSource<TResponse>> _Jobs =
				new Dictionary<string, TaskCompletionSource<TResponse>>();
			
			public Sender(string queueName, Func<TRequest, TResponse> localHandler ) : base(queueName)
			{
				AddInboundQueue(queueName, localHandler);
				AddResponseQueue(_QueueName, _Jobs);
			}

			public Task<TResponse> Send(IPAddress peerIP, TRequest body)
			{
				return Send<TRequest, TResponse>(peerIP, body, _Jobs);
			}
		}

		static IPAddress GetLocalIP()
		{
			var networkInterface = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(ni =>
			{
				if (ni.Name == "COMM")
					return true;

				return false;
			});

			try
			{
				if (networkInterface != null)
				{
					foreach (System.Net.NetworkInformation.UnicastIPAddressInformation IP in networkInterface.GetIPProperties().UnicastAddresses)
					{
						//Match address family
						if (IP.Address.AddressFamily != AddressFamily.InterNetwork)
							continue;

						//Check for IPv6 conditions since we can easily have multiple IPs
						if (IP.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 && (IP.Address.IsIPv6LinkLocal || IP.AddressPreferredLifetime != uint.MaxValue || IP.AddressValidLifetime != uint.MaxValue))
							continue;

						//We've found the IP
						return IP.Address;
					}
				}
			}
			catch (Exception e)
			{
				
			}
			return null;
		}
	}

	public class SenderInfo
	{
		public string OutboundQueuePath { get; set; }
		public string ResponseQueuePath { get; set; }
		public string ResponseAdminPath { get; set; }
		public MessageQueue OutboundQueue { get; set; }
	}
}
