using System;
using System.IO;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;

namespace ShardEqualizer.LocalStoring
{
	public class LocalStore<T>: LocalStore where T: Container
	{
		private readonly Action<T> _onSave;

		public LocalStore(string fileName, T container, Action<T> onSave): base(fileName)
		{
			_onSave = onSave;
			Container = container;
		}

		public T Container { get; }

		protected override Container GetUpdatedContainer()
		{
			_onSave?.Invoke(Container);
			return Container;
		}
	}

	public abstract class LocalStore
	{
		private static readonly JsonWriterSettings _jsonWriterSettings =
			new JsonWriterSettings() {Indent = true, OutputMode = JsonOutputMode.CanonicalExtendedJson};

		private readonly string _fileName;
		private volatile bool _changed = false;

		public LocalStore(string fileName)
		{
			_fileName = fileName;
		}

		public void OnChanged()
		{
			_changed = true;
		}

		protected abstract Container GetUpdatedContainer();

		public void SaveFile()
		{
			if(!_changed)
				return;

			var container = GetUpdatedContainer();
			var content = container.ToJson(container.GetType(), _jsonWriterSettings);
			File.WriteAllText(_fileName, content);
		}
	}
}
