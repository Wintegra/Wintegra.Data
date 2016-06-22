using System.Collections;
using System.Collections.Generic;

namespace Wintegra.Data.Tests.Entities
{
	public abstract class ENTRY_LIST<T> : IList<T>
	{
		protected ENTRY_LIST()
		{
			Items = new List<T>();
		}

		protected ENTRY_LIST(int capacity)
		{
			Items = new List<T>(capacity);
		}

		protected ENTRY_LIST(IEnumerable<T> collection)
		{
			Items = new List<T>(collection);
		}

		public List<T> Items { get; set; }

		public IEnumerator<T> GetEnumerator()
		{
			return Items.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Add(T item)
		{
			Items.Add(item);
		}

		public void Clear()
		{
			Items.Clear();
		}

		public bool Contains(T item)
		{
			return Items.Contains(item);
		}

		public void CopyTo(T[] array, int arrayIndex)
		{
			Items.CopyTo(array, arrayIndex);
		}

		public bool Remove(T item)
		{
			return Items.Remove(item);
		}

		public int Count
		{
			get { return Items.Count; }
		}

		public bool IsReadOnly
		{
			get { return ((ICollection<T>)Items).IsReadOnly; }
		}

		public int IndexOf(T item)
		{
			return Items.IndexOf(item);
		}

		public void Insert(int index, T item)
		{
			Items.Insert(index, item);
		}

		public void RemoveAt(int index)
		{
			Items.RemoveAt(index);
		}

		public T this[int index]
		{
			get { return Items[index]; }
			set { Items[index] = value; }
		}
	}
}