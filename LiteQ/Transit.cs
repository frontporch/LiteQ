﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace LiteQ
{
	public static class Transit
	{
		private static readonly Dictionary<string, MessageQueue> _InBound = new Dictionary<string, MessageQueue>();
		private static readonly Dictionary<int, SenderInfo> _Outbound = new Dictionary<int, SenderInfo>();
		private static readonly Dictionary<string, MessageQueue> _Administration = new Dictionary<string, MessageQueue>();
		private static JsonSerializer _JsonSerializer;

		private static readonly IPAddress _LocalIP = GetLocalIP();
		
		public static Sender<T> Register<T>(string queueName, Func<T,T> localHandler, Action<Message, T> responseHandler, JsonConverter[] converters = null, bool adminResponse = false)
		{
			return new Sender<T>(queueName, localHandler, responseHandler, converters);
		}

		public static Sender<TRequest, TResponse> Regsiter<TRequest, TResponse>(string queueName, Func<TRequest, TResponse> localHandler, Action<Message, TResponse> responseHandler, JsonConverter[] converters, bool adminResponse = false )
		{
			return new Sender<TRequest, TResponse>(queueName, localHandler, responseHandler, converters, adminResponse);
		}

		public class Sender<TRequest, TResponse>
		{
			private const int TIME_TO_LIVE = 5;
			
			private readonly string _QueueName;
			private readonly Func<TRequest, TResponse> _LocalHandler;
			private readonly Action<Message, TResponse> _ResponseHandler;
			private readonly bool _AdminResponse;
			public TimeSpan Timeout { get; set; }

			public Sender(string queueName, Func<TRequest, TResponse> localHandler, Action<Message, TResponse> responseHandler, JsonConverter[] converters, bool adminResponse)
			{
				_QueueName = queueName;
				Timeout = TimeSpan.FromSeconds(TIME_TO_LIVE);
				_LocalHandler = localHandler;
				_AdminResponse = adminResponse;
				_JsonSerializer = new JsonSerializer(converters);

				if(_AdminResponse)
					AddAdminQueue();

				AddInboundQueue();
				SetupResponseQueue();
				_ResponseHandler = responseHandler;
			}

			public string Send(IPAddress peerIP, TRequest body, TimeSpan? timeout = null, MessagePriority priority = MessagePriority.Normal)
			{
				try
				{
					int outboundKeyHash = (peerIP + _QueueName).GetHashCode();

					AddOutboundQueue(peerIP);

					string correlationId = Guid.NewGuid() + @"\" + "1";

					if (timeout.HasValue)
						Timeout = timeout.Value;

					Message message = new Message
					{
						AdministrationQueue = _AdminResponse ? new MessageQueue { Path = _Outbound[outboundKeyHash].ResponseAdminPath } : null,
						CorrelationId = correlationId,
						AcknowledgeType = _AdminResponse ? AcknowledgeTypes.PositiveArrival | AcknowledgeTypes.PositiveReceive : AcknowledgeTypes.None,
						TimeToBeReceived = Timeout,
						Priority = priority,
						ResponseQueue = new MessageQueue { Path = _Outbound[outboundKeyHash].ResponseQueuePath },
					};

					_JsonSerializer.SerializeMessageBody(message.BodyStream, body, typeof (TRequest), true);

					using (var transaction = new MessageQueueTransaction())
					{
						transaction.Begin();
						_Outbound[outboundKeyHash].OutboundQueue.Send(message, transaction);
						transaction.Commit();
					}

					return correlationId;
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
				});
			}

			private void SetupResponseQueue()
			{
				string responsePath = String.Format(@".\Private$\Response_{0}", _QueueName);

				if (!MessageQueue.Exists(responsePath))
					MessageQueue.Create(responsePath, true);

				var q = new MessageQueue
				{
					Path = responsePath,
					MessageReadPropertyFilter = new MessagePropertyFilter
					{
						Body = true,
						CorrelationId = true,
					},
				};

				q.ReceiveCompleted += (sender, args) => ResponseListener(sender, args, _ResponseHandler);
				q.BeginReceive(MessageQueue.InfiniteTimeout);
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

				if (message != null)
				{
					TRequest item = (TRequest)_JsonSerializer.Deserialize(typeof(TRequest), message.BodyStream);

					Message returnMessage = new Message
					{
						CorrelationId = message.CorrelationId
					};

					TResponse body = _LocalHandler(item);

					_JsonSerializer.SerializeMessageBody(returnMessage.BodyStream, body, typeof(TResponse), true);

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

			private void ResponseListener(object sender, ReceiveCompletedEventArgs receiveCompletedEventArgs, Action<Message, TResponse> responseHandler)
			{
				MessageQueue mq = (MessageQueue)sender;

				Message message = mq.EndReceive(receiveCompletedEventArgs.AsyncResult);

				if (message != null)
				{
					TResponse item = (TResponse) _JsonSerializer.Deserialize(typeof (TResponse), message.BodyStream);
					responseHandler(message, item);
				}
				
				mq.BeginReceive(MessageQueue.InfiniteTimeout);
			}
		}

		public class Sender<T> : Sender<T, T>
		{
			public Sender(string queueName, Func<T, T> localHandler, Action<Message, T> responseHandler, JsonConverter[] converters = null, bool adminResponse = false ) : base(queueName, localHandler, responseHandler, converters, adminResponse )
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

	public class ShippingStore<T> : ShippingStore<T, T>
	{
		public ShippingStore(string uniqueStoreName, Func<T, T> localHandler, JsonConverter[] converters = null, bool adminResponse = false)
			: base(uniqueStoreName, localHandler, converters, adminResponse)
		{	
		}
	}

	public class ShippingStore<TRequest, TResponse>
	{
		private readonly Dictionary<string, TaskCompletionSource<TResponse>> _Jobs =
			new Dictionary<string, TaskCompletionSource<TResponse>>();

		private readonly Transit.Sender<TRequest, TResponse> _Sender;

		public ShippingStore(string uniqueStoreName, Func<TRequest, TResponse> localHandler, JsonConverter[] converters = null, bool adminResponse = false)
		{
			_Sender = new Transit.Sender<TRequest, TResponse>(uniqueStoreName, localHandler, Finalize, converters, adminResponse);
		}

		public Task<TResponse> Send(IPAddress peerIP, TRequest body, TimeSpan? timeout = null, MessagePriority priority = MessagePriority.Normal)
		{
			TaskCompletionSource<TResponse> taskCompletionSource = new TaskCompletionSource<TResponse>();

			string correlationId = _Sender.Send(peerIP, body, timeout, priority);

			_Jobs.Add(correlationId, taskCompletionSource);

			return taskCompletionSource.Task;
		}

		public void Finalize(Message message, TResponse item)
		{
			_Jobs[message.CorrelationId].SetResult(item);
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
