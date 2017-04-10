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

namespace Mercent.AWS.Redshift
{
	/// <summary>
	/// Defines the data distribution style for the table.
	/// </summary>
	/// <remarks>
	/// Amazon Redshift distributes the rows of a table to the compute nodes
	/// according the distribution style specified for the table. The
	/// distribution style that you select for tables affects the overall
	/// performance of your database. For more information, see
	/// <a href="http://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html">Choosing a data distribution style</a>.
	/// </remarks>
	public enum DistributionStyle
	{
		/// <summary>
		/// The data in the table is spread evenly across the nodes
		/// in a cluster in a round-robin distribution.
		/// </summary>
		/// <remarks>
		/// Row IDs are used to determine the distribution, and roughly the
		/// same number of rows are distributed to each node.
		/// This is the default distribution method.
		/// </remarks>
		Even = 0,
		/// <summary>
		/// The data is distributed by the values in the DISTKEY column.
		/// </summary>
		/// <remarks>
		/// When you set the joining columns of joining tables as distribution keys, the joining rows from
		/// both tables are collocated on the compute nodes. When data is collocated, the optimizer can
		/// perform joins more efficiently. If you specify DISTSTYLE KEY, you must name a DISTKEY column,
		/// either for the table or as part of the column definition. For more information, see the
		/// DISTKEY keyword definition.
		/// </remarks>
		Key = 1,
		/// <summary>
		/// A copy of the entire table is distributed to every node.
		/// </summary>
		/// <remarks>
		/// This distribution style ensures that all the rows required for any join are available on every
		/// node, but it multiplies storage requirements and increases the load and maintenance times for
		/// the table. ALL distribution can improve execution time when used with certain dimension tables
		/// where KEY distribution is not appropriate, but performance improvements must be weighed
		/// against maintenance costs.
		/// </remarks>
		All = 8
	}
}
