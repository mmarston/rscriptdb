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
using System.Data;
using System.Linq;
using System.Reflection;

namespace Mercent.AWS.Redshift
{
	/// <summary>
	/// Base class of types that represent the ordinals for columns of a data record.
	/// </summary>
	/// <remarks>
	/// The inherited class should have public integer properties with getters and setters
	/// for each column ordinal needed.
	/// </remarks>
	internal abstract class DataRecordOrdinals
	{
	}

	internal static class DataRecordExtensions
	{
		public static T GetOrdinals<T>(this IDataRecord record)
			where T : DataRecordOrdinals, new()
		{
			if(record == null)
				throw new ArgumentNullException("record");

			T ordinals = new T();
			Type type = typeof(T);
			PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty | BindingFlags.SetProperty);
			foreach(PropertyInfo property in properties)
			{
				if(property.PropertyType == typeof(int))
				{
					property.SetValue(ordinals, record.GetOrdinal(property.Name), null);
				}
			}
			return ordinals;
		}
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting table column information.
	/// </summary>
	class ColumnInfoOrdinals : DataRecordOrdinals
	{
		public int ColumnName { get; set; }
		public int CompressionEncoding { get; set; }
		public int DataType { get; set; }
		public int DefaultValue { get; set; }
		public int Description { get; set; }
		public int IsDistributionKey { get; set; }
		public int IsNotNull { get; set; }
		public int Schema { get; set; }
		public int SortKeyNumber { get; set; }
		public int Parent { get; set; }
		public int ParentKind { get; set; }
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting table constraint information.
	/// </summary>
	class ConstraintInfoOrdinals : DataRecordOrdinals
	{
		public int ColumnNumbers { get; set; }
		public int ConstraintName { get; set; }
		public int ConstraintType { get; set; }
		public int Definition { get; set; }
		public int Description { get; set; }
		public int Schema { get; set; }
		public int Parent { get; set; }
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting the database information.
	/// </summary>
	class DatabaseInfoOrdinals : DataRecordOrdinals
	{
		public int AccessControlList { get; set; }
		public int DatabaseName { get; set; }
		public int Description { get; set; }
		public int Owner { get; set; }
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting the schema information.
	/// </summary>
	class SchemaInfoOrdinals : DataRecordOrdinals
	{
		public int AccessControlList { get; set; }
		public int Description { get; set; }
		public int Owner { get; set; }
		public int SchemaName { get; set; }
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting information about table row counts.
	/// </summary>
	class TableRowCountOrdinals : DataRecordOrdinals
	{
		public int EstimatedRowCount { get; set; }
		public int Schema { get; set; }
		public int Table { get; set; }
	}

	/// <summary>
	/// Provides the column ordinals for data reader to use when getting information about tables and views.
	/// </summary>
	class TableAndViewOrdinals : DataRecordOrdinals
	{
		public int AccessControlList { get; set; }
		public int Definition { get; set; }
		public int Description { get; set; }
		public int DistributionStyle { get; set; }
		public int EstimatedRowCount { get; set; }
		public int Kind { get; set; }
		public int Name { get; set; }
		public int Owner { get; set; }
		public int Schema { get; set; }
	}
}
