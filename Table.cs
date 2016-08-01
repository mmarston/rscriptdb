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
using System.Linq;

namespace Mercent.AWS.Redshift
{
	public class Table : NamedSchemaObject
	{
		public Table()
		{
			this.Columns = new ColumnCollection(this);
			this.Constraints = new ConstraintCollection(this);
		}

		public ColumnCollection Columns { get; private set; }
		public ConstraintCollection Constraints { get; private set; }

		public Column DistributionKey
		{
			get
			{
				return Columns.SingleOrDefault(c => c.IsDistributionKey);
			}
		}

		public DistributionStyle DistributionStyle { get; set; }

		public long EstimatedRowCount { get; set; }

		public Constraint PrimaryKey
		{
			get
			{
				return Constraints.SingleOrDefault(c => c.ConstraintType == ConstraintType.PrimaryKey);
			}
		}

		public IEnumerable<Constraint> UniqueConstraints()
		{
			return Constraints.Where(c => c.ConstraintType == ConstraintType.Unique);
		}

		public IEnumerable<Constraint> ForeignKeyConstraints()
		{
			return Constraints.Where(c => c.ConstraintType == ConstraintType.ForeignKey);
		}

		public IEnumerable<Column> SortKeys()
		{
			return
				from c in this.Columns
				where c.SortKeyNumber > 0
				orderby c.SortKeyNumber
				select c;
		}
	}
}
