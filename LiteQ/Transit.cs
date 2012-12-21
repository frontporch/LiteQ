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

		public class Sender<TRequest, TResponse>
		{
			private readonly string _QueueName;
			private readonly Func<TRequest, TResponse> _LocalHandler;

			private readonly Dictionary<string, TaskCompletionSource<TResponse>> _Jobs =
				new Dictionary<string, TaskCompletionSource<TResponse>>();
			
			private readonly MessageQueue _ResponseQueue;

			public Sender(string queueName, Func<TRequest, TResponse> localHandler)
			{
				_QueueName = queueName;
				_LocalHandler = localHandler;

				AddAdminQueue();
				AddInboundQueue();
				_ResponseQueue = SetupResponseQueue();
			}

			public Task<TResponse> Send(IPAddress peerIP, TRequest body)
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

					_Jobs.Add(correlationId, taskCompletionSource);

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

			private void AddOutboundQueue(IPAddress peerIP)
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
			
			private void AddAdminQueue()
			{
				string administrationQueuePath = String.Format(@".\Private$\Administration_{0}", _QueueName);

				if (!MessageQueue.Exists(administrationQueuePath))
					MessageQueue.Create(administrationQueuePath);

				_Administration.Add(_QueueName, new MessageQueue
				{
					Path = administrationQueuePath,
					Formatter = new BinaryMessageFormatter()
				});
			}

			private MessageQueue SetupResponseQueue()
			{
				string responsePath = String.Format(@".\Private$\Response_{0}", _QueueName);

				if (!MessageQueue.Exists(responsePath))
					MessageQueue.Create(responsePath, true);

				var q = new MessageQueue
				{
					Path = responsePath,
					Formatter = new BinaryMessageFormatter(),
					MessageReadPropertyFilter = new MessagePropertyFilter
					{
						Body = true,
						CorrelationId = true,
					},
				};

				q.ReceiveCompleted += ResponseListener;
				q.BeginReceive(MessageQueue.InfiniteTimeout);

				return q;
			}

			private void AddInboundQueue()
			{
				if (_InBound.ContainsKey(_QueueName))
					throw new ArgumentException("Queue with that name already exits");

				string queuePath = String.Format(@".\Private$\{0}", _QueueName);

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

				inboundQ.ReceiveCompleted += ListenReceiver;
				inboundQ.BeginReceive(MessageQueue.InfiniteTimeout);

				_InBound.Add(_QueueName, inboundQ);
			}

			private void ListenReceiver(object sender, ReceiveCompletedEventArgs receiveCompletedEventArgs)
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
							Body = _LocalHandler(item),
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

			private void ResponseListener(object sender, ReceiveCompletedEventArgs receiveCompletedEventArgs)
			{
				MessageQueue mq = (MessageQueue)sender;

				try
				{
					Message message = mq.EndReceive(receiveCompletedEventArgs.AsyncResult);

					if (message != null)
					{
						TResponse item = (TResponse)message.Body;
						_Jobs[message.CorrelationId].SetResult(item);
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

		public class Sender<T> : Sender<T, T>
		{
			public Sender(string queueName, Func<T, T> localHandler ) : base(queueName, localHandler)
			{
			}
		}


		static IPAddress GetLocalIP()
		{
			var networkInterface = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(ni => ni.Name == "COMM");

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
