using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Net;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace LiteQ
{
	public class JsonSerializer
	{
		private static JsonConverter[] _Converters;
		private static Newtonsoft.Json.JsonSerializer _Serializer;
		private static Newtonsoft.Json.JsonSerializer _Deserializer;

		public JsonSerializer(JsonConverter[] converters)
		{
			_Converters = converters;
			_Serializer = new Newtonsoft.Json.JsonSerializer();
			_Deserializer = new Newtonsoft.Json.JsonSerializer();
			_Serializer.NullValueHandling = NullValueHandling.Include;
			_Deserializer.NullValueHandling = NullValueHandling.Include;

			if (_Converters != null)
			{
				foreach (var converter in _Converters)
				{
					_Serializer.Converters.Add(converter);
					_Deserializer.Converters.Add(converter);
				}
			}

			// Only give out strict ISO-8601
			_Serializer.Converters.Add(new IsoDateTimeConverter { DateTimeFormat = "o" });

			_Serializer.Converters.Add(new UriConverter());

			// Parse just about anything for input
			_Deserializer.Converters.Add(new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AdjustToUniversal });

			// custom handling ip address keys :(
			_Deserializer.Converters.Add(new DictionaryIPAddressKeyConverter());
		}

		public void Serialize(Stream destination, object item, Type type, bool indented)
		{
			using (var sw = new StreamWriter(destination))
			{
				_Serializer.Formatting = indented ? Formatting.Indented : Formatting.None;
				_Serializer.Serialize(sw, item);
			}
		}

		public void SerializeMessageBody(Stream destination, object item, Type type, bool indented)
		{
			var sw = new StreamWriter(destination);
			_Serializer.Formatting = indented ? Formatting.Indented : Formatting.None;
			_Serializer.Serialize(sw, item);
			sw.Flush();
		}

		public object Deserialize(Type type, Stream source)
		{
			try
			{
				using (var sr = new StreamReader(source))
				using (var jr = new JsonTextReader(sr))
				{
					return _Deserializer.Deserialize(jr, type);
				}
			}
			catch (JsonSerializationException jsonException)
			{
				throw new SerializationException("Deserialization failed", jsonException);
			}
			catch (JsonReaderException jsonException)
			{
				throw new SerializationException("Deserialization failed", jsonException);
			}
			catch (ArgumentException jsonException)
			{
				// It feels like a bug in Newtonsoft that its enum converter just throws ArgumentException rather than
				// a Json*Exception
				throw new SerializationException("Deserialization failed", jsonException);
			}
			catch (FormatException jsonException)
			{
				throw new SerializationException("Deserialization failed", jsonException);
			}
			catch (OverflowException jsonException)
			{
				throw new SerializationException("Deserialization failed", jsonException);
			}
		}

		public static string SerializeToString(object item)
		{
			var sw = new StringWriter();
			_Serializer.Serialize(sw, item);
			return sw.ToString();
		}

		public static string SerializeToString(object item, Type type)
		{
			return SerializeToString(item);
		}

		public static object DeserializeFromString(string source, Type type)
		{
			return _Deserializer.Deserialize(new StringReader(source), type);
		}

		#region Converters

		public class UriConverter : JsonConverter
		{
			public override bool CanConvert(Type objectType)
			{
				return objectType == typeof(Uri);
			}

			public override object ReadJson(JsonReader reader, Type objectType, object existingValue, Newtonsoft.Json.JsonSerializer serializer)
			{
				throw new NotImplementedException();
			}

			public override void WriteJson(JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
			{
				writer.WriteValue(((Uri)value).OriginalString);
			}
		}

		public class DictionaryIPAddressKeyConverter : JsonConverter
		{
			public override bool CanConvert(Type objectType)
			{
				return objectType.IsGenericType
					&& objectType.IsGenericType
					&& objectType.GetGenericTypeDefinition() == typeof(Dictionary<,>)
					&& objectType.GetGenericArguments()[0] == typeof(IPAddress);
			}

			// caching ctor and Add method should help a ton -> delegates
			public override object ReadJson(JsonReader reader, Type objectType, object existingValue, Newtonsoft.Json.JsonSerializer serializer)
			{
				Type valueType = objectType.GetGenericArguments()[1];
				object dict = objectType.GetConstructor(Type.EmptyTypes).Invoke(null);
				MethodInfo addMethod = objectType.GetMethod("Add");

				// Console.WriteLine("{0} {1} {2} {3}", valueType, reader.Value, reader.ValueType, reader.TokenType);

				if (reader.TokenType != JsonToken.StartObject)
				{
					throw new JsonReaderException("Expected Newtonsoft.Json.JsonToken.StartObject");
				}

				reader.Read();
				while (reader.TokenType != JsonToken.EndObject)
				{
					if (reader.TokenType != JsonToken.PropertyName)
					{
						throw new JsonReaderException("Expected Newtonsoft.Json.JsonToken.PropertyName");
					}
					IPAddress key = IPAddress.Parse((string)reader.Value);
					reader.Read();

					// Console.WriteLine("before value {0} {1} {2}", reader.Value, reader.ValueType, reader.TokenType);
					object value = serializer.Deserialize(reader, valueType);

					addMethod.Invoke(dict, new object[] { key, value });

					reader.Read();
					// Console.WriteLine("after add {0} {1} {2}", reader.Value, reader.ValueType, reader.TokenType);
				}

				//reader.Value
				return dict;
			}

			public override void WriteJson(JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
			{
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
