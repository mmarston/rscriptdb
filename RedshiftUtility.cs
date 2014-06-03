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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;

namespace Mercent.AWS.Redshift
{
	public static class RedshiftUtility 
	{
		#region Reserved Words (HashSet<string> reservedWords)
		
		// See http://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html
		static HashSet<string> reservedWords = new HashSet<string>
		(
			new []
			{
				"AES128",
				"AES256",
				"ALL",
				"ALLOWOVERWRITE",
				"ANALYSE",
				"ANALYZE",
				"AND",
				"ANY",
				"ARRAY",
				"AS",
				"ASC",
				"AUTHORIZATION",
				"BACKUP",
				"BETWEEN",
				"BINARY",
				"BLANKSASNULL",
				"BOTH",
				"BYTEDICT",
				"CASE",
				"CAST",
				"CHECK",
				"COLLATE",
				"COLUMN",
				"CONSTRAINT",
				"CREATE",
				"CREDENTIALS",
				"CROSS",
				"CURRENT_DATE",
				"CURRENT_TIME",
				"CURRENT_TIMESTAMP",
				"CURRENT_USER",
				"CURRENT_USER_ID",
				"DEFAULT",
				"DEFERRABLE",
				"DEFLATE",
				"DEFRAG",
				"DELTA",
				"DELTA32K",
				"DESC",
				"DISABLE",
				"DISTINCT",
				"DO",
				"ELSE",
				"EMPTYASNULL",
				"ENABLE",
				"ENCODE",
				"ENCRYPT",
				"ENCRYPTION",
				"END",
				"EXCEPT",
				"EXPLICIT",
				"FALSE",
				"FOR",
				"FOREIGN",
				"FREEZE",
				"FROM",
				"FULL",
				"GLOBALDICT256",
				"GLOBALDICT64K",
				"GRANT",
				"GROUP",
				"GZIP",
				"HAVING",
				"IDENTITY",
				"IGNORE",
				"ILIKE",
				"IN",
				"INITIALLY",
				"INNER",
				"INTERSECT",
				"INTO",
				"IS",
				"ISNULL",
				"JOIN",
				"LEADING",
				"LEFT",
				"LIKE",
				"LIMIT",
				"LOCALTIME",
				"LOCALTIMESTAMP",
				"LUN",
				"LUNS",
				"LZO",
				"LZOP",
				"MINUS",
				"MOSTLY13",
				"MOSTLY32",
				"MOSTLY8",
				"NATURAL",
				"NEW",
				"NOT",
				"NOTNULL",
				"NULL",
				"NULLS",
				"OFF",
				"OFFLINE",
				"OFFSET",
				"OLD",
				"ON",
				"ONLY",
				"OPEN",
				"OR",
				"ORDER",
				"OUTER",
				"OVERLAPS",
				"PARALLEL",
				"PARTITION",
				"PERCENT",
				"PLACING",
				"PRIMARY",
				"RAW",
				"READRATIO",
				"RECOVER",
				"REFERENCES",
				"REJECTLOG",
				"RESORT",
				"RESTORE",
				"RIGHT",
				"SELECT",
				"SESSION_USER",
				"SIMILAR",
				"SOME",
				"SYSDATE",
				"SYSTEM",
				"TABLE",
				"TAG",
				"TDES",
				"TEXT255",
				"TEXT32K",
				"THEN",
				"TO",
				"TOP",
				"TRAILING",
				"TRUE",
				"TRUNCATECOLUMNS",
				"UNION",
				"UNIQUE",
				"USER",
				"USING",
				"VERBOSE",
				"WALLET",
				"WHEN",
				"WHERE",
				"WITH",
				"WITHOUT"
			},
			StringComparer.OrdinalIgnoreCase
		);

		#endregion

		/// <summary>
		/// Regex for a safe (standard) identifier that does not need to be quoted.
		/// </summary>
		/// <remarks>
		/// Must start with ASCII letter or underscore then can be followed
		/// by ASCII letters, numbers, underscore or dollar sign.
		/// This pattern does not check whether the identifier is a reserved word.
		/// See http://docs.aws.amazon.com/redshift/latest/dg/r_names.html
		/// </remarks>
		static Regex safeIdentifierRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_$]*$");

		public static NpgsqlDataReader ExecuteReader(NpgsqlConnection connection, string query, params NpgsqlParameter[] parameters)
		{
			if(connection == null)
				throw new ArgumentNullException("connection");
			if(query == null)
				throw new ArgumentNullException("query");

			NpgsqlCommand command = null;
			try
			{
				command = new NpgsqlCommand(query, connection);
				if(parameters != null)
					command.Parameters.AddRange(parameters);
				return command.ExecuteReader();
			}
			catch(Exception)
			{
				if(command != null)
					command.Dispose();
				throw;
			}
		}

		public static string GetQualifiedName(string parentName, string localName, QuoteMode mode = QuoteMode.WhenNecessary)
		{
			if(String.IsNullOrEmpty(parentName))
				return GetQuotedIdentifier(localName, mode, false);
			else
				return GetQuotedIdentifier(parentName, mode, false) + '.' + GetQuotedIdentifier(localName, mode, true);
		}

		public static string GetQuotedIdentifier(string identifier, QuoteMode mode = QuoteMode.Always, bool qualified = false)
		{
			if(identifier == null)
				throw new ArgumentNullException("identifier");

			// If the quote mode is Always or if the mode is WhenNecessary
			// and it is necessary to quote the identifier (because it is not safe)
			// then quote the identifier by surrounding with double quotes and
			// replacing all occurances of double quotes with a pair of double quotes.
			if(mode == QuoteMode.Always || (mode == QuoteMode.WhenNecessary && !IsSafeIdentifier(identifier, qualified)))
				return '"' + identifier.Replace("\"", "\"\"") + '"';
			else
				return identifier;
		}

		public static bool IsReservedWord(string identifier)
		{
			return reservedWords.Contains(identifier);
		}

		public static bool IsSafeIdentifier(string identifier, bool qualified = false)
		{
			if(identifier == null)
				throw new ArgumentNullException("identifier");

			// The identifier is safe it it matches the regex pattern
			// and will either be qualified or is not a reserved word.
			// Redshift allows a reserved word to be used as an identifier if
			// it is either quoted or qualified. For example, GROUP is a reserved word,
			// but it can be used as a column name in the product table if quoted like "group"
			// or qualified like product.group.
			return safeIdentifierRegex.IsMatch(identifier) &&
				(qualified || !IsReservedWord(identifier));
		}

		/// <summary>
		/// Updates the connection string with the password from the pgpass file if the password is missing from the connection string.
		/// </summary>
		public static string UpdatePassword(string connectionString)
		{
			var builder = new NpgsqlConnectionStringBuilder(connectionString);
			if(UpdatePassword(builder))
				return builder.ToString();
			else
				return connectionString;
		}

		/// <summary>
		/// Updates the connection string builder with the password from the pgpass file if the password is missing from the connection string.
		/// </summary>
		/// <returns>true if the password was updated</returns>
		public static bool UpdatePassword(NpgsqlConnectionStringBuilder builder)
		{
			if(builder == null)
				throw new ArgumentNullException("builder");

			// Don't change the connection string if it already contains a password.
			byte[] password = builder.PasswordAsByteArray;
			if(password != null && password.Length > 0)
				return false;

			// Find the first matching entry.
			PGPassEntry entry = PGPassEntries().FirstOrDefault(e => e.IsMatch(builder));

			// If a match was found then use the password from the matching entry.
			if(entry == null)
				return false;
			else
			{
				builder[Keywords.Password] = entry.Password;
				return true;
			}
		}

		static IEnumerable<PGPassEntry> PGPassEntries()
		{
			string fileName = @"%APPDATA%\postgresql\pgpass.conf";
			fileName = Environment.ExpandEnvironmentVariables(fileName);

			// If the file does not exist then return an empty enumerable.
			if(!File.Exists(fileName))
				return Enumerable.Empty<PGPassEntry>();

			return
				from line in File.ReadLines(fileName)
				select new PGPassEntry(line);
		}

		/// <summary>
		/// Represents an entry in the pgpass file.
		/// </summary>
		/// <remarks>
		/// See http://www.postgresql.org/docs/8.4/static/libpq-pgpass.html
		/// </remarks>
		class PGPassEntry
		{
			public PGPassEntry(string entry)
			{
				if(entry == null)
					throw new ArgumentNullException("entry");
				List<string> parts = Parse(entry);
				if(parts.Count < 5)
					throw new ArgumentException("Could not pgpass file parse entry.");
				Host = parts[0];
				if(parts[1] != "*")
					Port = Int32.Parse(parts[1]);
				Database = parts[2];
				UserName = parts[3];
				Password = parts[4];
			}

			public string Database { get; private set; }
			public string Host { get; private set; }
			public string Password { get; private set; }
			public int? Port { get; private set; }
			public string UserName { get; private set; }

			public bool IsMatch(NpgsqlConnectionStringBuilder builder)
			{
				return
					IsMatch(Host, builder.Host)
					&& (!Port.HasValue || Port == builder.Port)
					&& IsMatch(Database, builder.Database)
					&& IsMatch(UserName, builder.UserName);
			}

			static bool IsMatch(string pattern, string value)
			{
				return pattern == "*" || String.Equals(pattern, value, StringComparison.OrdinalIgnoreCase);
			}

			List<string> Parse(string entry)
			{
				List<string> parts = new List<string>();
				StringBuilder builder = new StringBuilder();
				IEnumerator<char> chars = entry.GetEnumerator();
				while(chars.MoveNext())
				{
					char ch = chars.Current;
					if(ch == '\'' && chars.MoveNext())
						builder.Append(chars.Current);
					else if(ch == ':')
					{
						parts.Add(builder.ToString());
						builder.Clear();
					}
					else
						builder.Append(ch);
				}
				parts.Add(builder.ToString());
				return parts;
			}
		}
	}
}
