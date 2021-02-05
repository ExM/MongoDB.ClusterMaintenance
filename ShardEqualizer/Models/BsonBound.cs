using System;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;

namespace ShardEqualizer.Models
{
	[BsonSerializer(typeof(Serializer))]
	public readonly struct BsonBound: IEquatable<BsonBound>, IComparable<BsonBound>
	{
		public BsonBound(BsonDocument value)
		{
			_value = value ?? throw new ArgumentNullException(nameof(value));
		}

		private readonly BsonDocument _value;

		public static explicit operator BsonDocument(BsonBound source)
		{
			return source._value;
		}

		public static bool operator ==(BsonBound x, BsonBound y)
		{
			return x.Equals(y);
		}

		public static bool operator !=(BsonBound x, BsonBound y)
		{
			return !(x == y);
		}

		public bool Equals(BsonBound other)
		{
			return other._value.Equals(_value);
		}

		public override bool Equals(object obj)
		{
			return obj is BsonBound a && Equals(a);
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		public int CompareTo(BsonBound other)
		{
			return _value.CompareTo(other._value);
		}

		public static bool operator >(BsonBound x, BsonBound y)
		{
			return x.CompareTo(y) > 0;
		}

		public static bool operator <(BsonBound x, BsonBound y)
		{
			return x.CompareTo(y) < 0;
		}

		public static bool operator >=(BsonBound x, BsonBound y)
		{
			return x.CompareTo(y) >= 0;
		}

		public static bool operator <=(BsonBound x, BsonBound y)
		{
			return x.CompareTo(y) <= 0;
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		public static explicit operator BsonBound(BsonDocument v)
		{
			return new BsonBound(v);
		}

		internal class Serializer : StructSerializerBase<BsonBound>
		{
			private static readonly BsonDocumentSerializer _serializer = new BsonDocumentSerializer();

			private static readonly BsonDeserializationArgs _deserializationArgs =
				new BsonDeserializationArgs()
				{
					NominalType = typeof(BsonDocument)
				};

			private static readonly BsonSerializationArgs _serializationArgs =
				new BsonSerializationArgs()
				{
					NominalType = typeof(BsonDocument),
					SerializeIdFirst = true,
					SerializeAsNominalType = true
				};

			public override BsonBound Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
			{
				var doc = _serializer.Deserialize(context, _deserializationArgs);
				return new BsonBound(doc);
			}

			public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, BsonBound value)
			{
				_serializer.Serialize(context, _serializationArgs, value._value);
			}
		}

		public static BsonBound Parse(string text)
		{
			var result = TryParse(text);
			if (result == null)
				throw new FormatException($"text '{text}' does not contain the correct BSON bound value");

			return result.Value;
		}

		public static BsonBound? TryParse(string text)
		{
			if(string.IsNullOrWhiteSpace(text))
				return null;

			using var jsonReader = new JsonReader(text);

			var bsonDocument = BsonDocumentSerializer.Instance.Deserialize(BsonDeserializationContext.CreateRoot(jsonReader));
			if (!jsonReader.IsAtEndOfFile())
				throw new FormatException("String contains extra non-whitespace characters beyond the end of the document.");
			return new BsonBound(bsonDocument);
		}
	}
}

