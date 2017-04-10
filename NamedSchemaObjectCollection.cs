//   Copyright © 2005-2016 Commerce Technologies, LLC
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
	public class NamedSchemaObjectCollection<T> : KeyedCollection<string, T>
		where T : NamedSchemaObject
	{
		Schema owner;

		protected NamedSchemaObjectCollection(Schema owner)
			: base(StringComparer.OrdinalIgnoreCase)
		{
			if(owner == null)
				throw new ArgumentNullException("owner");
			this.owner = owner;
		}

		protected override void ClearItems()
		{
			foreach(T item in this)
				item.Schema = null;
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

			if(item.Schema != null)
			{
				string message;
				if(item.Schema != owner)
				{
					message = String.Format("Cannot add \'{0}\' to schema \'{1}\' because it already belongs to schema \'{2}\'.", item.Name, owner.Name, item.Schema.Name);
					throw new ArgumentException(message, "item");
				}
				if(Contains(item))
				{
					message = String.Format("Cannot insert \'{0}\' into schema \'{1}\' at index {3} because it already belongs to the schema.", item.Name, owner.Name, index);
					throw new ArgumentException(message, "item");
				}
			}
			base.InsertItem(index, item);
			item.Schema = owner;
		}

		protected override void RemoveItem(int index)
		{
			this[index].Schema = null;
			base.RemoveItem(index);
		}

		protected override void SetItem(int index, T item)
		{
			throw new NotSupportedException("This collection does not allow setting an object at a specific index. Use the Add method instead.");
		}
	}
}
