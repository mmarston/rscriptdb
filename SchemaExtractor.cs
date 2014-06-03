using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Mercent.AWS.Redshift
{
	/// <summary>
	/// Extracts the schema from an online database.
	/// </summary>
	public class SchemaExtractor : IDisposable
	{
		NpgsqlConnection connection;

		public SchemaExtractor(string connectionString)
		{
			if(connectionString == null)
				throw new ArgumentNullException("connectionString");
			connection = new NpgsqlConnection(connectionString);
			connection.Open();
		}

		public string ConnectionString { get; private set; }

		public Database GetDatabase(string name)
		{
			if(name == null)
				throw new ArgumentNullException("name");

			Database database = LoadDatabase(name);
			LoadGroups(database);
			LoadSchemas(database);
			LoadTablesAndViews(database);
			LoadColumns(database);
			LoadConstraints(database);
			return database;
		}

		NpgsqlDataReader ExecuteReader(string query, params NpgsqlParameter[] parameters)
		{
			return RedshiftUtility.ExecuteReader(connection, query, parameters);
		}

		/// <summary>
		/// Gets a nullable boolean from a record and using default value when null.
		/// </summary>
		/// <remarks>
		/// Use the <see cref="IDataRecord.GetBoolean"/> method directly for column that should not be null (it will throw an exception).
		/// </remarks>
		bool GetBoolean(IDataRecord record, int ordinal, bool defaultValue)
		{
			if(record.IsDBNull(ordinal))
				return defaultValue;
			else
				return record.GetBoolean(ordinal);
		}

		ConstraintType GetConstraintType(IDataRecord record, int ordinal)
		{
			return GetConstraintType(record.GetChar(ordinal));
		}

		ConstraintType GetConstraintType(Char ch)
		{
			switch(ch)
			{
				case 'f':
					return ConstraintType.ForeignKey;
				case 'p':
					return ConstraintType.PrimaryKey;
				case 'u':
					return ConstraintType.Unique;
				default:
					throw new ArgumentException("Unrecognized constraint type: " + ch, "ch");
			}
		}

		DistributionStyle GetDistributionStyle(IDataRecord record, int ordinal)
		{
			// Default to Even.
			if(record.IsDBNull(ordinal))
				return DistributionStyle.Even;

			// The pg_class.reldiststyle column returns a 16 bit int.
			short value = record.GetInt16(ordinal);

			// The DistributeStyle enum values have been set to match the values defined by Redshift.
			return (DistributionStyle)value;
		}

		/// <summary>
		/// Gets a nullable string from a record and returns the optional default value when null.
		/// </summary>
		/// <remarks>
		/// Use the <see cref="IDataRecord.GetString"/> method directly for column that should not be null (it will throw an exception).
		/// </remarks>
		string GetString(IDataRecord record, int ordinal, string defaultValue = null)
		{
			if(record.IsDBNull(ordinal))
				return defaultValue;
			else
				return record.GetString(ordinal);
		}

		void LoadColumns(Database database)
		{
			string query =
@"
SELECT
	nsp.nspname AS Schema,
	c.relname AS Parent,
	c.relkind AS ParentKind,
	a.attname AS ColumnName,
	format_type(a.atttypid, a.atttypmod) AS DataType,
	a.attnotnull AS IsNotNull,
	pg_get_expr(ad.adbin, ad.adrelid) AS DefaultValue,
	d.description,
	format_encoding(a.attencodingtype) AS CompressionEncoding,
	a.attisdistkey AS IsDistributionKey,
	a.attsortkeyord AS SortKeyNumber
FROM pg_catalog.pg_attribute AS a
	INNER JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
	INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = c.relnamespace
	LEFT JOIN pg_catalog.pg_attrdef AS ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
	LEFT JOIN pg_catalog.pg_description AS d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE a.attnum > 0
	AND NOT a.attisdropped
	AND c.relkind IN ('r', 'v')
	AND nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_toast')
	AND nsp.nspname NOT LIKE 'pg_temp_%'
ORDER BY nsp.nspname, c.relname, a.attnum;
";
			using(NpgsqlDataReader reader = ExecuteReader(query))
			{
				ColumnInfoOrdinals ordinals = reader.GetOrdinals<ColumnInfoOrdinals>();
				while(reader.Read())
				{
					string schemaName = reader.GetString(ordinals.Schema);
					var schema = database.Schemas[schemaName];

					string parentName = reader.GetString(ordinals.Parent);
					char parentKind = reader.GetChar(ordinals.ParentKind);

					var column = new Column
					{
						Name = reader.GetString(ordinals.ColumnName),
						CompressionEncoding = GetString(reader, ordinals.CompressionEncoding),
						DataType = reader.GetString(ordinals.DataType),
						DefaultValue = GetString(reader, ordinals.DefaultValue),
						Description = GetString(reader, ordinals.Description),
						IsDistributionKey = reader.GetBoolean(ordinals.IsDistributionKey),
						IsNullable = !reader.GetBoolean(ordinals.IsNotNull),
						SortKeyNumber = reader.GetInt32(ordinals.SortKeyNumber)
					};

					if(parentKind == 'r')
					{
						var table = schema.Tables[parentName];
						table.Columns.Add(column);
					}
					else if(parentKind == 'v')
					{
						var view = schema.Views[parentName];
						view.Columns.Add(column);
					}
				}
			}
		}

		void LoadConstraints(Database database)
		{
			string query =
@"
SELECT
	nsp.nspname AS Schema,
	c.relname AS Parent,
	con.conname AS ConstraintName,
	con.contype AS ConstraintType,
	con.conkey AS ColumnNumbers,
	pg_get_constraintdef(con.oid, true) AS Definition,
	d.description
FROM pg_catalog.pg_constraint AS con
	INNER JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid
	INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = c.relnamespace
	LEFT JOIN pg_catalog.pg_description AS d ON d.objoid = con.oid AND d.objsubid = 0
WHERE c.relkind = 'r'
	AND nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_toast')
	AND nsp.nspname NOT LIKE 'pg_temp_%'
ORDER BY nsp.nspname, c.relname, con.conname;
";
			using(NpgsqlDataReader reader = ExecuteReader(query))
			{
				ConstraintInfoOrdinals ordinals = reader.GetOrdinals<ConstraintInfoOrdinals>();
				while(reader.Read())
				{
					string schemaName = reader.GetString(ordinals.Schema);
					var schema = database.Schemas[schemaName];

					string parentName = reader.GetString(ordinals.Parent);
					var table = schema.Tables[parentName];

					var constraint = new Constraint
					{
						Name = reader.GetString(ordinals.ConstraintName),
						ConstraintType = GetConstraintType(reader, ordinals.ConstraintType),
						Definition = reader.GetString(ordinals.Definition),
						Description = GetString(reader, ordinals.Description)
					};

					short[] columnNumbers = reader.GetValue(ordinals.ColumnNumbers) as short[];
					if(columnNumbers != null)
					{
						// Because Redshift is a columnar store the column actually gets removed
						// when dropped from the table so it no long shows up in the pg_catalog.pg_attribute
						// system catalog. Because of this we can use the column number - 1 as the index into table.Columns.
						// Otherwise the column would have continue to show up in pg_catalog.pg_attribute
						// with attisdropped = true and the attnum would not correspond to the index.
						foreach(short columnNumber in columnNumbers)
						{
							var column = table.Columns[columnNumber - 1];
							constraint.Columns.Add(column);
						}
					}

					table.Constraints.Add(constraint);
				}
			}
		}

		Database LoadDatabase(string name)
		{
			string query =
@"
SELECT
	dat.datname AS DatabaseName,
	u.usename AS Owner,
	array_to_string(dat.datacl, '\n') AS AccessControlList,
	d.description
FROM pg_catalog.pg_database AS dat
	INNER JOIN pg_catalog.pg_user AS u ON u.usesysid = dat.datdba
	LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = dat.oid AND d.objsubid = 0
WHERE dat.datname = :databaseName;
";
			using(NpgsqlDataReader reader = ExecuteReader(query, new NpgsqlParameter("databaseName", name)))
			{
				DatabaseInfoOrdinals ordinals = reader.GetOrdinals<DatabaseInfoOrdinals>();
				while(reader.Read())
				{
					var database = new Database
					{
						Name = reader.GetString(ordinals.DatabaseName),
						Owner = reader.GetString(ordinals.Owner),
						AccessControlList = GetString(reader, ordinals.AccessControlList),
						Description = GetString(reader, ordinals.Description) 
					};
					return database;
				}
				throw new ArgumentException("Database with name '" + name + "' was not found (or the user does not have permissions).", "name");
			}
		}

		void LoadGroups(Database database)
		{
			string query =
@"
SELECT groname AS Name
FROM pg_catalog.pg_group
ORDER BY groname;
";
			using(NpgsqlDataReader reader = ExecuteReader(query))
			{
				while(reader.Read())
				{
					var group = new Group
					{
						Name = reader.GetString(0)
					};
					database.Groups.Add(group);
				}
			}
		}

		void LoadSchemas(Database database)
		{
			string query =
@"
SELECT
	nsp.nspname AS SchemaName,
	u.usename AS Owner,
	array_to_string(nsp.nspacl, '\n') AS AccessControlList,
	d.description
FROM pg_catalog.pg_namespace AS nsp
	INNER JOIN pg_catalog.pg_user AS u ON u.usesysid = nsp.nspowner
	LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = nsp.oid AND d.objsubid = 0
WHERE nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_toast')
	AND nsp.nspname NOT LIKE 'pg_temp_%'
ORDER BY nsp.nspname;
";
			using(NpgsqlDataReader reader = ExecuteReader(query))
			{
				SchemaInfoOrdinals ordinals = reader.GetOrdinals<SchemaInfoOrdinals>();
				while(reader.Read())
				{
					var schema = new Schema
					{
						Name = reader.GetString(ordinals.SchemaName),
						Owner = reader.GetString(ordinals.Owner),
						AccessControlList = GetString(reader, ordinals.AccessControlList),
						Description = GetString(reader, ordinals.Description) 
					};
					database.Schemas.Add(schema);
				}
			}
		}

		void LoadTablesAndViews(Database database)
		{
			string query =
@"
SELECT
	c.relname AS Name,
	c.relkind AS Kind,
	nsp.nspname AS Schema,
	u.usename AS Owner,
	array_to_string(c.relacl, '\n') AS AccessControlList,
	c.reldiststyle AS DistributionStyle,
	c.reltuples::bigint AS EstimatedRowCount,
	CASE
		WHEN c.relkind = 'v' THEN pg_get_viewdef(c.oid, true)
	END AS Definition,
	d.description
FROM pg_catalog.pg_class AS c
	INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = c.relnamespace
	INNER JOIN pg_catalog.pg_user AS u ON u.usesysid = c.relowner
	LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = c.oid AND d.objsubid = 0
WHERE c.relkind IN ('r', 'v')
	AND nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_toast')
	AND nsp.nspname NOT LIKE 'pg_temp_%'
ORDER BY nsp.nspname, c.relname;
";

			// I wasn't able to get this query to be combined with the query above:
			//SELECT id, SUM(rows) AS rows
			//FROM pg_catalog.stv_tbl_perm
			//GROUP BY id

			using(NpgsqlDataReader reader = ExecuteReader(query))
			{
				TableAndViewOrdinals ordinals = reader.GetOrdinals<TableAndViewOrdinals>();
				while(reader.Read())
				{
					string schemaName = reader.GetString(ordinals.Schema);
					var schema = database.Schemas[schemaName];
					
					char kind = reader.GetChar(ordinals.Kind);
					if(kind == 'r')
					{
						var table = new Table
						{
							Name = reader.GetString(ordinals.Name),
							Owner = reader.GetString(ordinals.Owner),
							AccessControlList = GetString(reader, ordinals.AccessControlList),
							Description = GetString(reader, ordinals.Description),
							DistributionStyle = GetDistributionStyle(reader, ordinals.DistributionStyle),
							EstimatedRowCount = reader.GetInt64(ordinals.EstimatedRowCount)
						};
						schema.Tables.Add(table);
					}
					else if(kind == 'v')
					{
						var view = new View
						{
							Name = reader.GetString(ordinals.Name),
							Owner = reader.GetString(ordinals.Owner),
							AccessControlList = GetString(reader, ordinals.AccessControlList),
							Description = GetString(reader, ordinals.Description),
							Definition = reader.GetString(ordinals.Definition)
						};
						schema.Views.Add(view);
					}
				}
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			if(connection != null)
			{
				connection.Close();
				connection = null;
			}
		}

		#endregion
	}
}
