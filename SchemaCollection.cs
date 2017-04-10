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
	public class SchemaCollection : KeyedCollection<string, Schema>
	{
		Database owner;

		internal SchemaCollection(Database owner)
			: base(StringComparer.OrdinalIgnoreCase)
		{
			if(owner == null)
				throw new ArgumentNullException("owner");
			this.owner = owner;
		}

		protected override void ClearItems()
		{
			foreach(Schema schema in this)
				schema.Database = null;
			base.ClearItems();
		}

		protected override string GetKeyForItem(Schema schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");
			return schema.Name;
		}

		protected override void InsertItem(int index, Schema schema)
		{
			if(schema == null)
				throw new ArgumentNullException("schema");

			if(schema.Database != null)
			{
				string message;
				if(schema.Database != owner)
				{
					message = String.Format("Cannot add schema \'{0}\' to database \'{1}\' because the schema already belongs to database \'{2}\'.", schema.Name, owner.Name, schema.Database.Name);
					throw new ArgumentException(message, "schema");
				}
				if(Contains(schema))
				{
					message = String.Format("Cannot insert schema \'{0}\' into database \'{1}\' at index {3} because it already belongs to the database.", schema.Name, owner.Name, index);
					throw new ArgumentException(message, "schema");
				}
			}
			base.InsertItem(index, schema);
			schema.Database = owner;
		}

		protected override void RemoveItem(int index)
		{
			this[index].Database = null;
			base.RemoveItem(index);
		}

		protected override void SetItem(int index, Schema schema)
		{
			throw new NotSupportedException("This collection does not allow setting an object at a specific index. Use the Add method instead.");
		}
	}
}
