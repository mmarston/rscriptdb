//   Copyright 2014 Mercent Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Mercent.AWS.Redshift
{
	public abstract class NamedChildObjectCollection<T> : KeyedCollection<string, T>
		where T: NamedChildObject
	{
		NamedSchemaObject parent;

		internal NamedChildObjectCollection(NamedSchemaObject parent)
			: base(StringComparer.OrdinalIgnoreCase)
		{
			if(parent == null)
				throw new ArgumentNullException("parent");
			this.parent = parent;
		}

		protected override void ClearItems()
		{
			foreach(T item in this)
				item.Parent = null;
			base.ClearItems();
		}

		protected override string GetKeyForItem(T item)
		{
			if(item == null)
				throw new ArgumentNullException("item");
			return item.Name;
		}

		protected override void InsertItem(int index, T item)
		{
			if(item == null)
				throw new ArgumentNullException("item");

			if(item.Parent != null)
			{
				string message;
				if(item.Parent != this.parent)
				{
					message = String.Format("Cannot add \'{0}\' to \'{1}\' because the item already belongs to \'{2}\'.", item.Name, this.parent, item.Parent);
					throw new ArgumentException(message, "item");
				}
				if(Contains(item))
				{
					message = String.Format("Cannot insert \'{0}\' into \'{1}\' at index {3} because it is already contained in the collection.", item.Name, this.parent, index);
					throw new ArgumentException(message, "item");
				}
			}
			base.InsertItem(index, item);
			item.Parent = this.parent;
		}

		protected override void RemoveItem(int index)
		{
			this[index].Parent = null;
			base.RemoveItem(index);
		}

		protected override void SetItem(int index, T item)
		{
			throw new NotSupportedException("This collection does not allow setting an object at a specific index. Use the Add or Insert instead.");
		}
	}
}
