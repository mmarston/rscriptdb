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
using Npgsql;
using NpgsqlTypes;

namespace Mercent.AWS.Redshift
{
	public class FileScripter : IDisposable
	{
		static readonly HashSet<string> knownExtensions = new HashSet<string>
		(
			new[] { ".sql" },
			StringComparer.OrdinalIgnoreCase
		);

		Char allEmptyDirectoriesResponseChar = '\0';
		Char allExtraFilesResponseChar = '\0';
		NpgsqlConnection connection;
		HashSet<string> fileSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		SortedSet<string> ignoreFileSet = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
		bool ignoreFileSetModified = false;
		List<ScriptFile> scriptFiles = new List<ScriptFile>();

		public FileScripter(string connectionString)
		{
			if(connectionString == null)
				throw new ArgumentNullException("connectionString");

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder(connectionString);
			if(String.IsNullOrEmpty(builder.Database))
				throw new ArgumentException("The database name must be included in the connection string.", "connectionString");
			
			this.DatabaseName = builder.Database;

			// Set defaults.
			this.Encoding = new UTF8Encoding(false);
			this.MaxRowCount = 100000;
			this.OutputDirectory = String.Empty;
			this.QuoteMode = QuoteMode.WhenNecessary;

			// Open the connection.
			connection = new NpgsqlConnection(connectionString);
			connection.Open();
		}

		public event EventHandler<MessageReceivedEventArgs> ErrorMessageReceived;
		public event EventHandler<MessageReceivedEventArgs> OutputMessageReceived;
		public event EventHandler<MessageReceivedEventArgs> ProgressMessageReceived;

		/// <summary>
		/// Gets the name of the database that is being scripted.
		/// </summary>
		/// <remarks>
		/// This is set in the constructor based on the connection string.
		/// </remarks>
		public string DatabaseName { get; private set; }

		public Encoding Encoding { get; set; }

		/// <summary>
		/// Force the scripter to continue even when errors or data loss may occur.
		/// </summary>
		/// <remarks>
		/// Set this to <c>true</c> or <c>false</c> when running using automation tools
		/// that don't have an user interaction. This avoids prompting the user.
		/// This setting affects any errors where the user would normally be given the option
		/// to continue or abort (a "prompted" error). It also suppresses prompting the user for what to do with
		/// extra files. When set to <c>true</c> the scripter will continue on prompted
		/// errors and will delete extra files. When set to <c>false</c> the scripter
		/// will abort on prompted errors and keep extra files.
		/// </remarks>
		public bool? ForceContinue { get; set; }

		/// <summary>
		/// Gets or sets the maximum number of rows to export.
		/// </summary>
		/// <remarks>
		/// This is used to prevent exporting data from a table that has a large number of rows.
		/// The default value is 100,000.
		/// </remarks>
		public int MaxRowCount { get; set; }

		public string OutputDirectory { get; set; }

		public QuoteMode QuoteMode { get; set; }

		public void Script()
		{
			Database database;
			using(SchemaExtractor extractor = new SchemaExtractor(this.connection.ConnectionString))
			{
				database = extractor.GetDatabase(this.DatabaseName);
			}
			Script(database);
		}

		public void Script(Database database)
		{
			if(database == null)
				throw new ArgumentNullException("database");
			VerifyProperties();

			scriptFiles.Clear();
			ignoreFileSet.Clear();
			ignoreFileSetModified = false;
			fileSet.Clear();
			if(!ForceContinue.HasValue)
			{
				allEmptyDirectoriesResponseChar = '\0';
				allExtraFilesResponseChar = '\0';
			}
			else if(ForceContinue.Value)
			{
				allEmptyDirectoriesResponseChar = 'd';
				allExtraFilesResponseChar = 'd';
			}
			else
			{
				allEmptyDirectoriesResponseChar = 'k';
				allExtraFilesResponseChar = 'k';
			}

			if(this.OutputDirectory.Length > 0 && !Directory.Exists(this.OutputDirectory))
				Directory.CreateDirectory(this.OutputDirectory);

			ScriptDatabase(database);
			// We don't currently script out groups because they are shared at
			// the server level and do not belong to a database.
			//ScriptGroups(database);
			ScriptSchemas(database);
			ScriptViewHeaders(database);
			ScriptTables(database);
			ScriptViews(database);

			using(StreamWriter writer = new StreamWriter(Path.Combine(OutputDirectory, "CreateDatabaseObjects.sql"), false, Encoding))
			{
				writer.WriteLine(@"\set ON_ERROR_STOP on");
				foreach(ScriptFile file in this.scriptFiles.Where(f => f.Command != null))
				{
					writer.WriteLine();
					writer.WriteLine(@"\echo '{0}'", file.FileName.Replace('\\', '/'));
					writer.WriteLine(file.Command);
				}
			}

			AddScriptFile("CreateDatabaseObjects.sql", null);

			DirectoryInfo outputDirectoryInfo;
			if(OutputDirectory != "")
				outputDirectoryInfo = new DirectoryInfo(OutputDirectory);
			else
				outputDirectoryInfo = new DirectoryInfo(".");

			// Prompt the user for what to do with extra files.
			// When objects are deleted from the database ensure that the user
			// wants to delete the corresponding files. There may also be other
			// files in the directory that are not scripted files.

			AddIgnoreFiles();
			PromptExtraFiles(outputDirectoryInfo, "");
			SaveIgnoreFiles();
		}

		void AddIgnoreFiles()
		{
			string ignoreFileName = Path.Combine(OutputDirectory, "IgnoreFiles.txt");
			AddScriptFile("IgnoreFiles.txt", null);
			if(File.Exists(ignoreFileName))
			{
				foreach(string line in File.ReadAllLines(ignoreFileName))
				{
					string ignoreLine = line.Trim();
					ignoreFileSet.Add(ignoreLine);
					if(ignoreLine.Contains("*"))
					{
						string directory = OutputDirectory;
						string filePattern = ignoreLine;
						string[] parts = ignoreLine.Split('\\', '/');
						if(parts.Length > 0)
						{
							string[] dirs = parts.Take(parts.Length - 1).ToArray();
							directory = Path.Combine(OutputDirectory, Path.Combine(dirs));
							filePattern = parts.Last();
						}
						if(Directory.Exists(directory))
						{
							foreach(string fileName in Directory.EnumerateFiles(directory, filePattern))
							{
								// Get the path to the fileName relative to the OutputDirectory.
								string relativePath = fileName.Substring(OutputDirectory.Length).TrimStart('/', '\\');
								AddScriptFile(relativePath, null);
							}
						}
					}
					else
						AddScriptFile(ignoreLine, null);
				}
			}
			// Ignore the bin and obj directories of an SSDT project.
			// It doesn't hurt to always ignore these, so no need
			// to wrap this in a check for if(TargetDataTools)...
			AddScriptFile("bin", null);
			AddScriptFile("obj", null);
		}

		void AddScriptFile(ScriptFile scriptFile)
		{
			if(scriptFile == null)
				throw new ArgumentNullException("scriptFile");
			this.scriptFiles.Add(scriptFile);
			if(scriptFile.FileName != null)
				this.fileSet.Add(scriptFile.FileName);
		}

		void AddScriptFile(string fileName)
		{
			AddScriptFile(new ScriptFile(fileName));
		}

		void AddScriptFile(string fileName, string command)
		{
			AddScriptFile(new ScriptFile(fileName, command));
		}

		void AddScriptFileRange(IEnumerable<string> fileNames)
		{
			foreach(string fileName in fileNames)
				AddScriptFile(fileName);
		}

		void AppendChecksum(StringBuilder builder, Table table)
		{
			// Use either primary key, unique key, distribution key or all collumns (listed in order of priority).
			Constraint primaryKey = table.PrimaryKey;
			if(primaryKey != null)
			{
				AppendChecksum(builder, primaryKey.Columns);
				return;
			}

			Constraint uniqueKey = table.UniqueConstraints().FirstOrDefault();
			if(uniqueKey != null)
			{
				AppendChecksum(builder, uniqueKey.Columns);
				return;
			}

			Column distributionKey = table.DistributionKey;
			if(distributionKey != null)
			{
				AppendChecksum(builder, new[] { distributionKey });
				return;
			}

			AppendChecksum(builder, table.Columns);
		}

		void AppendChecksum(StringBuilder builder, IEnumerable<Column> columns)
		{
			builder.Append("CHECKSUM(('' ");
			foreach(Column column in columns)
			{
				builder.Append(" || ");
				if(column.IsNullable)
					builder.AppendFormat("COALESCE({0}, '')", column.GetQuotedName(this.QuoteMode));
				else
					builder.Append(column.GetQuotedName(this.QuoteMode));
			}
			builder.Append(")::varchar(max))");
		}

		void AppendColumns(StringBuilder builder, IEnumerable<Column> columns, string delimiter = ", ")
		{
			string innerDelimiter = null;
			foreach(var column in columns)
			{
				if(innerDelimiter != null)
					builder.Append(innerDelimiter);
				else
					innerDelimiter = delimiter;
				builder.Append(column.GetQuotedName(this.QuoteMode));
			}
		}

		void AppendOrderBy(StringBuilder builder, Table table)
		{
			// Use either primary key, unique key, or all columns (listed in order of priority).
			builder.Append("ORDER BY ");

			Constraint primaryKey = table.PrimaryKey;
			if(primaryKey != null)
			{
				AppendColumns(builder, primaryKey.Columns);
				return;
			}

			Constraint uniqueKey = table.UniqueConstraints().FirstOrDefault();
			if(uniqueKey != null)
			{
				AppendColumns(builder, uniqueKey.Columns);
				return;
			}

			AppendColumns(builder, table.Columns);
		}

		NpgsqlDataReader ExecuteReader(string query, params NpgsqlParameter[] parameters)
		{
			return RedshiftUtility.ExecuteReader(connection, query, parameters);
		}

		string GetInsertClause(Table table)
		{
			StringBuilder builder = new StringBuilder();
			builder.AppendFormat("INSERT INTO {0}\r\n(\r\n\t", table.GetQualifiedName(this.QuoteMode));
			AppendColumns(builder, table.Columns, ",\r\n\t");
			builder.Append("\r\n)");
			return builder.ToString();
		}

		IEnumerable<string> GetPrivileges(string privilegeCodes)
		{
			foreach(char code in privilegeCodes)
			{
				// See #define ACL_INSERT_CHR (and other definitions) in http://doxygen.postgresql.org/acl_8h_source.html
				switch(code)
				{
					case 'a':
						yield return "INSERT";
						break;
					case 'r':
						yield return "SELECT";
						break;
					case 'w':
						yield return "UPDATE";
						break;
					case 'd':
						yield return "DELETE";
						break;
					case 'D':
						// Redshift doesn't allow explicitly granting the TRUNCATE privilege.
						// yield return "TRUNCATE";
						break;
					case 'x':
						yield return "REFERENCES";
						break;
					case 't':
						// Redshift doesn't allow explicitly granting the TRIGGER privilege.
						// yield return "TRIGGER";
						break;
					case 'E':
						yield return "EXECUTE";
						break;
					case 'U':
						yield return "USAGE";
						break;
					case 'C':
						yield return "CREATE";
						break;
					case 'T':
						yield return "CREATE TEMP";
						break;
					case 'c':
						yield return "CONNECT";
						break;
				}
			}
		}

		string GetQuotedIdentifier(string identifier)
		{
			return RedshiftUtility.GetQuotedIdentifier(identifier, this.QuoteMode);
		}

		string GetSelectCommand(Table table)
		{
			StringBuilder selectCommand = new StringBuilder();
			selectCommand.AppendFormat("SELECT TOP {0} *,\r\n\t", this.MaxRowCount);
			AppendChecksum(selectCommand, table);
			selectCommand.AppendFormat("\r\nFROM {0}\r\n", table.GetQualifiedName(this.QuoteMode));
			AppendOrderBy(selectCommand, table);
			selectCommand.AppendLine(";");
			return selectCommand.ToString();
		}

		void OnErrorMessageReceived(string message)
		{
			if(ErrorMessageReceived == null)
				Console.Error.WriteLine(message);
			else
				ErrorMessageReceived(this, new MessageReceivedEventArgs(message));
		}

		void OnProgressMessageReceived(string message)
		{
			if(ProgressMessageReceived == null)
			{
				// For the console indicate progress with a period when the message is null.
				if(message == null)
					Console.Write('.');
				else
					Console.WriteLine(message);
			}
			else
				ProgressMessageReceived(this, new MessageReceivedEventArgs(message));
		}

		void PromptExtraFiles(DirectoryInfo dirInfo, string relativeDir)
		{
			string relativeName;
			foreach(FileInfo fileInfo in dirInfo.GetFiles())
			{
				// Skip over the file if it isn't a known extension (.sql, .dat, .udat, .fmt).
				if(!knownExtensions.Contains(fileInfo.Extension))
					continue;
				relativeName = Path.Combine(relativeDir, fileInfo.Name);
				if(!fileSet.Contains(relativeName))
				{
					Console.WriteLine("Extra file: {0}", relativeName);
					char responseChar = this.allExtraFilesResponseChar;
					if(allExtraFilesResponseChar == '\0')
					{
						Console.WriteLine("Keep, delete, or ignore this file? For all extra files? (press k, d, i, or a)");
						ConsoleKeyInfo key = Console.ReadKey(true);
						responseChar = key.KeyChar;
						if(responseChar == 'a')
						{
							Console.WriteLine("Keep, delete, or ignore all remaining extra files? (press k, d, i)");
							key = Console.ReadKey(true);
							responseChar = key.KeyChar;
							// Only accept the response char if it is k, d, or i.
							// Other characters are ignored, which is the same as keeping this file.
							if(responseChar == 'k' || responseChar == 'd' || responseChar == 'i')
								allExtraFilesResponseChar = responseChar;
						}
					}
					if(responseChar == 'd')
					{
						try
						{
							fileInfo.Delete();
							Console.WriteLine("Deleted file.");
						}
						catch(Exception ex)
						{
							Console.WriteLine("Delete failed. {0}: {1}", ex.GetType().Name, ex.Message);
						}
					}
					else if(responseChar == 'i')
					{
						ignoreFileSetModified = true;
						ignoreFileSet.Add(relativeName);
					}
				}
			}
			foreach(DirectoryInfo subDirInfo in dirInfo.GetDirectories())
			{
				string relativeSubDir = Path.Combine(relativeDir, subDirInfo.Name);
				// Skip the directory if it is hidden or in the file set (because it was in the ignore list).
				if(subDirInfo.Attributes.HasFlag(FileAttributes.Hidden) || fileSet.Contains(relativeSubDir))
					continue;
				// If the directory is not empty then recursively call PromptExtraFiles...
				if(subDirInfo.EnumerateFileSystemInfos().Any())
					PromptExtraFiles(subDirInfo, relativeSubDir);
				else
				{
					// If the directory is empty, prompt about deleting it.
					Console.WriteLine("Empty directory: {0}", relativeSubDir);
					char responseChar = this.allEmptyDirectoriesResponseChar;
					if(allEmptyDirectoriesResponseChar == '\0')
					{
						Console.WriteLine("Keep, delete, or ignore this directory? For all empty directories? (press k, d, i, or a)");
						ConsoleKeyInfo key = Console.ReadKey(true);
						responseChar = key.KeyChar;
						if(responseChar == 'a')
						{
							Console.WriteLine("Keep, delete, or ignore all remaining empty directories? (press k, d, i)");
							key = Console.ReadKey(true);
							responseChar = key.KeyChar;
							// Only accept the response char if it is k, d, or i.
							// Other characters are ignored, which is the same as keeping this directory.
							if(responseChar == 'k' || responseChar == 'd' || responseChar == 'i')
								allEmptyDirectoriesResponseChar = responseChar;
						}
					}
					if(responseChar == 'd')
					{
						try
						{
							subDirInfo.Delete();
							Console.WriteLine("Deleted directory.");
						}
						catch(Exception ex)
						{
							Console.WriteLine("Delete failed. {0}: {1}", ex.GetType().Name, ex.Message);
						}
					}
					else if(responseChar == 'i')
					{
						ignoreFileSetModified = true;
						ignoreFileSet.Add(relativeSubDir);
					}
				}
			}
		}

		void SaveIgnoreFiles()
		{
			if(ignoreFileSetModified)
			{
				string ignoreFileName = Path.Combine(OutputDirectory, "IgnoreFiles.txt");
				File.WriteAllLines(ignoreFileName, this.ignoreFileSet);
			}
		}

		void ScriptAccessControlList(TextWriter writer, Database database, bool newline = false)
		{
			string grantedObject = String.Format("DATABASE {0}", database.GetQuotedName(this.QuoteMode));
			ScriptAccessControlList(writer, grantedObject, database.AccessControlList, newline);
		}

		void ScriptAccessControlList(TextWriter writer, Schema schema, bool newline = false)
		{
			string grantedObject = String.Format("SCHEMA {0}", schema.GetQuotedName(this.QuoteMode));
			ScriptAccessControlList(writer, grantedObject, schema.AccessControlList, newline);
		}

		void ScriptAccessControlList(TextWriter writer, Table table, bool newline = false)
		{
			string grantedObject = String.Format("TABLE {0}", table.GetQualifiedName(this.QuoteMode));
			ScriptAccessControlList(writer, grantedObject, table.AccessControlList, newline);
		}

		void ScriptAccessControlList(TextWriter writer, View view, bool newline = false)
		{
			string grantedObject = view.GetQualifiedName(this.QuoteMode);
			ScriptAccessControlList(writer, grantedObject, view.AccessControlList, newline);
		}

		void ScriptAccessControlList(TextWriter writer, string grantedObject, string accessControlList, bool newline = false)
		{
			if(accessControlList == null)
				return;

			var entries =
				from entry in accessControlList.Split('\n')
				let entryParts = entry.Split('=', '/')
				let grantee = entryParts[0]
				// Only include privileges granted to groups.
				where grantee.StartsWith("group ", StringComparison.OrdinalIgnoreCase)
				orderby grantee
				select new { Grantee = entryParts[0], PrivilegeCodes = entryParts[1] };

			foreach(var entry in entries)
			{
				if(newline)
					writer.WriteLine();
				else
					newline = true;

				writer.Write("GRANT ");
				WriteRange(writer, GetPrivileges(entry.PrivilegeCodes), ", ");
				writer.WriteLine(" ON {0} TO {1};", grantedObject, entry.Grantee);
			}
		}

		void ScriptConstraint(TextWriter writer, Constraint constraint)
		{
			writer.WriteLine
			(
				"ALTER TABLE {0} ADD CONSTRAINT {1} {2};",
				constraint.Parent.GetQualifiedName(this.QuoteMode),
				constraint.GetQuotedName(this.QuoteMode),
				constraint.Definition
			);
			ScriptDescription(writer, constraint, true);
		}

		void ScriptConstraints(TextWriter writer, IEnumerable<Constraint> constraints, bool newline = false)
		{
			foreach(var constraint in constraints)
			{
				if(newline)
					writer.WriteLine();
				else
					newline = true;
				ScriptConstraint(writer, constraint);
			}
		}

		void ScriptDatabase(Database database)
		{
			string fileName = "Database.sql";
			string outputFileName = Path.Combine(this.OutputDirectory, fileName);

			OnProgressMessageReceived(fileName);

			using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				writer.Write("CREATE DATABASE :newdbname");
				if(database.Owner != null)
					writer.Write(" WITH OWNER {0}", GetQuotedIdentifier(database.Owner));
				writer.WriteLine(';');
				ScriptDescription(writer, database, true);
				ScriptAccessControlList(writer, database, true);
				writer.WriteLine();
				writer.WriteLine(@"\c :newdbname");
			}

			AddScriptFile(fileName);
		}

		void ScriptDescription(TextWriter writer, Column column, bool newline = false)
		{
			if(column.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine("COMMENT ON COLUMN {0} IS", column.GetQualifiedName(this.QuoteMode));
				WriteStringLiteral(writer, column.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescription(TextWriter writer, Constraint constraint, bool newline = false)
		{
			if(constraint.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine
				(
					"COMMENT ON CONSTRAINT {0} ON {1} IS",
					constraint.GetQuotedName(this.QuoteMode),
					constraint.Parent.GetQualifiedName(this.QuoteMode)
				);
				WriteStringLiteral(writer, constraint.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescription(TextWriter writer, Database database, bool newline = false)
		{
			if(database.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine("COMMENT ON DATABASE {0} IS", database.GetQuotedName(this.QuoteMode));
				WriteStringLiteral(writer, database.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescription(TextWriter writer, Schema schema, bool newline = false)
		{
			if(schema.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine("COMMENT ON SCHEMA {0} IS", schema.GetQuotedName(this.QuoteMode));
				WriteStringLiteral(writer, schema.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescription(TextWriter writer, Table table, bool newline = false)
		{
			if(table.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine("COMMENT ON TABLE {0} IS", table.GetQualifiedName(this.QuoteMode));
				WriteStringLiteral(writer, table.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescription(TextWriter writer, View view, bool newline = false)
		{
			if(view.Description != null)
			{
				if(newline)
					writer.WriteLine();
				writer.WriteLine("COMMENT ON VIEW {0} IS", view.GetQualifiedName(this.QuoteMode));
				WriteStringLiteral(writer, view.Description);
				writer.WriteLine(';');
			}
		}

		void ScriptDescriptions(TextWriter writer, IEnumerable<Column> columns, bool newline = false)
		{
			foreach(var column in columns)
			{
				if(column.Description != null)
				{
					if(newline)
						writer.WriteLine();
					else
						newline = true;
					ScriptDescription(writer, column);
				}
			}
		}

		void ScriptSchema(TextWriter writer, Schema schema)
		{
			writer.Write("CREATE SCHEMA ");
			writer.Write(schema.GetQuotedName(this.QuoteMode));
			if(schema.Owner != null)
			{
				writer.Write(" AUTHORIZATION ");
				writer.Write(GetQuotedIdentifier(schema.Owner));
			}
			writer.WriteLine(';');
			ScriptDescription(writer, schema);
			ScriptAccessControlList(writer, schema, true);
		}

		void ScriptSchemas(Database database)
		{
			Directory.CreateDirectory(Path.Combine(OutputDirectory, "Schemas"));
			string fileName = @"Schemas\Schemas.sql";
			string outputFileName = Path.Combine(this.OutputDirectory, fileName);

			OnProgressMessageReceived(fileName);
			using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				bool hasPublicSchema = false;
				bool newline = false;
				foreach(var schema in database.Schemas)
				{
					// The public schema is automatically created when the database is created.
					// It will cause an error if we try to create it.
					if(String.Equals(schema.Name, "public", StringComparison.OrdinalIgnoreCase))
					{
						hasPublicSchema = true;
						if(schema.Description != null)
						{
							ScriptDescription(writer, schema, newline);
							newline = true;
						}
					}
					else
					{
						if(newline)
							writer.WriteLine();
						else
							newline = true;
						ScriptSchema(writer, schema);
					}
				}
				// If the database that we are scripting out does not have a public schema
				// then script a DROP SCHEMA statement to drop the public schema when creating a new database.
				if(!hasPublicSchema)
				{
					if(newline)
						writer.WriteLine();
					writer.WriteLine("DROP SCHEMA public;");
				}
			}
			AddScriptFile(fileName);
		}

		void ScriptTable(TextWriter writer, Table table)
		{
			// Start of CREATE TABLE statement.
			writer.WriteLine("CREATE TABLE {0}", table.GetQualifiedName(this.QuoteMode));
			writer.Write("(\r\n\t");
			ScriptTableColumns(writer, table);
			writer.Write(')');

			// Distribution Style.
			// Note that we don't script DISTSTYLE EVEN because that is the default.
			if(table.DistributionStyle == DistributionStyle.All)
				writer.Write("\r\nDISTSTYLE ALL");
			else if(table.DistributionStyle == DistributionStyle.Key)
			{
				writer.Write("\r\nDISTKEY(");
				writer.Write(table.DistributionKey.GetQuotedName(this.QuoteMode));
				writer.Write(')');
			}

			// Sort Key.
			string sortKeyColumnNames = String.Join(", ", table.SortKeys().Select(c => c.GetQuotedName(this.QuoteMode)));
			if(sortKeyColumnNames.Length > 0)
			{
				writer.Write("\r\nSORTKEY(");
				writer.Write(sortKeyColumnNames);
				writer.Write(')');
			}

			// End of CREATE TABLE statement.
			writer.WriteLine(';');

			if(table.Owner != null)
			{
				writer.WriteLine();
				writer.WriteLine("ALTER TABLE {0} OWNER TO {1};", table.GetQualifiedName(this.QuoteMode), GetQuotedIdentifier(table.Owner));
			}

			// Table Description.
			ScriptDescription(writer, table, true);

			// Column Descriptions.
			ScriptDescriptions(writer, table.Columns, true);

			// Primary Key.
			Constraint primaryKey = table.PrimaryKey;
			if(primaryKey != null)
			{
				writer.WriteLine();
				ScriptConstraint(writer, primaryKey);
			}

			// Unique Constraints.
			ScriptConstraints(writer, table.UniqueConstraints(), true);

			// Access Control List (Grant Privileges).
			ScriptAccessControlList(writer, table, true);
		}

		void ScriptTableColumn(TextWriter writer, Column column)
		{
			// Name
			writer.Write(column.GetQuotedName(this.QuoteMode));
			writer.Write('\t');

			// Data Type
			writer.Write(column.DataType);

			// Nullable
			if(column.IsNullable)
				writer.Write("\tNULL");
			else
				writer.Write("\tNOT NULL");

			// Default Value
			if(column.DefaultValue != null)
			{
				writer.Write('\t');
				if(!column.DefaultValue.StartsWith("IDENTITY(", StringComparison.OrdinalIgnoreCase))
					writer.Write("DEFAULT ");
				writer.Write(column.DefaultValue);
			}

			// Compression Encoding
			if(column.HasCompressionEncoding)
			{
				writer.Write("\tENCODE ");
				writer.Write(column.CompressionEncoding.ToUpper());
			}
		}

		void ScriptTableColumns(TextWriter writer, Table table)
		{
			string delimiter = null;
			foreach(var column in table.Columns)
			{
				if(delimiter == null)
					delimiter = ",\r\n\t";
				else
					writer.Write(delimiter);
				ScriptTableColumn(writer, column);
			}
			writer.WriteLine();
		}

		void ScriptTableData(Table table)
		{
			// If the table does not have any rows then skip querying it for data.
			if(table.EstimatedRowCount == 0)
				return;

			// If the table has over MaxRowCount rows then output a warning and skip it.
			if(table.EstimatedRowCount > this.MaxRowCount)
			{
				string warning = String.Format
				(
					"Warning: Skipping data export of table {0}.{1} because the table has an estimated {2:N0} rows. Any table with more than {3:N0} rows will be skipped.",
					table.Schema.Name,
					table.Name,
					table.EstimatedRowCount,
					this.MaxRowCount
				);
				OnErrorMessageReceived(warning);
				return;
			}

			string relativeDir = Path.Combine("Schemas", table.Schema.Name, "Data");
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, table.Name + ".sql");
			string outputFileName = Path.Combine(OutputDirectory, fileName);
			AddScriptFile(fileName);
			OnProgressMessageReceived(fileName);

			string selectCommand = GetSelectCommand(table);

			string insertClause = GetInsertClause(table);
			
			using(NpgsqlDataReader reader = ExecuteReader(selectCommand))
			using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				const int maxBatchSize = 1000;
				const int divisor = 511;
				const int remainder = 510;

				int checksumOrdinal = reader.FieldCount - 1;
				object[] values = new object[reader.FieldCount];
				NpgsqlDbType[] types = new NpgsqlDbType[reader.FieldCount];
				for(int i = 0; i < reader.FieldCount; i++)
				{
					types[i] = reader.GetFieldNpgsqlDbType(i);
				}
				bool isFirstBatch = true;
				int rowCount = 0;
				IList<Column> columns = table.Columns;

				// Note that we don't currently properly handle tables with an identity column.
				while(reader.Read())
				{
					int checksum = reader.GetInt32(checksumOrdinal);
					if(checksum % divisor == remainder || rowCount % maxBatchSize == 0)
					{
						// Reset rowCount for the start of a new batch.
						rowCount = 0;
						// If this isn't the first batch then we want to output ";" to separate the batches.
						if(isFirstBatch)
							isFirstBatch = false;
						else
							writer.WriteLine(";");
						writer.WriteLine();
						writer.Write(insertClause);
						writer.Write(" VALUES\r\n(\r\n\t");
					}
					else
						writer.Write(",\r\n(\r\n\t");

					reader.GetValues(values);
					for(int i = 0; i < columns.Count; i++)
					{
						if(i > 0)
							writer.Write(",\r\n\t");
						string castType = rowCount == 0 ? columns[i].DataType : null;
						WriteLiteral(writer, values[i], types[i]);
					}

					writer.Write("\r\n)");
					rowCount++;
				}

				writer.WriteLine(';');
				writer.WriteLine();
				writer.WriteLine("VACUUM {0};", table.GetQualifiedName(this.QuoteMode));
				writer.WriteLine();
				writer.WriteLine("ANALYZE {0};", table.GetQualifiedName(this.QuoteMode));
			}
		}

		void ScriptTables(Database database)
		{
			List<string> fkyFileNames = new List<string>();

			foreach(var schema in database.Schemas)
			{
				// Skip the schema if it does not have any tables
				// (don't create an empty Tables directory).
				if(!schema.Tables.Any())
					continue;

				string relativeDir = Path.Combine("Schemas", schema.Name, "Tables");
				string dir = Path.Combine(OutputDirectory, relativeDir);
				if(!Directory.Exists(dir))
					Directory.CreateDirectory(dir);

				foreach(var table in schema.Tables)
				{
					string fileName = Path.Combine(relativeDir, table.Name + ".sql");
					string outputFileName = Path.Combine(OutputDirectory, fileName);
					AddScriptFile(fileName);
					OnProgressMessageReceived(fileName);

					using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						ScriptTable(writer, table);
					}

					ScriptTableData(table);

					fileName = Path.Combine(relativeDir, table.Name + ".fky.sql");
					outputFileName = Path.Combine(OutputDirectory, fileName);
					fkyFileNames.Add(fileName);
					OnProgressMessageReceived(fileName);

					using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
					{
						ScriptConstraints(writer, table.ForeignKeyConstraints());
					}
				}
			}

			AddScriptFileRange(fkyFileNames);
		}

		void ScriptView(TextWriter writer, View view)
		{
			writer.WriteLine("CREATE OR REPLACE VIEW {0}\r\nAS\r\n{1}", view.GetQualifiedName(this.QuoteMode), view.Definition);

			if(view.Owner != null)
			{
				writer.WriteLine();
				// Note that Redshift doesn't currently support the ALTER VIEW statement,
				// but it does let you use ALTER TABLE even though it references a view.
				writer.WriteLine("ALTER TABLE {0} OWNER TO {1};", view.GetQualifiedName(this.QuoteMode), GetQuotedIdentifier(view.Owner));
				ScriptAccessControlList(writer, view, true);
			}

			ScriptDescription(writer, view, true);
		}

		void ScriptViewHeader(TextWriter writer, View view)
		{
			writer.WriteLine("CREATE OR REPLACE VIEW {0}\r\nAS", view.GetQualifiedName(this.QuoteMode));

			writer.Write("SELECT\r\n\t");
			string delimiter = null;
			foreach(Column column in view.Columns)
			{
				if(delimiter == null)
					delimiter = ",\r\n\t";
				else
					writer.Write(delimiter);
				writer.Write("NULL::{0} AS {1}", column.DataType, column.GetQuotedName(this.QuoteMode));
			}
			writer.WriteLine(";");
		}

		void ScriptViewHeaders(Database database)
		{
			foreach(var schema in database.Schemas)
				ScriptViewHeaders(schema);
		}

		void ScriptViewHeaders(Schema schema)
		{
			// Skip the schema if it does not have any views
			// (don't create an empty Views.sql file).
			if(!schema.Views.Any())
				return;

			string relativeDir = Path.Combine("Schemas", schema.Name, "Views");
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			string fileName = Path.Combine(relativeDir, "Views.sql");
			string outputFileName = Path.Combine(OutputDirectory, fileName);
			OnProgressMessageReceived(fileName);

			using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
			{
				bool newline = false;
				foreach(var view in schema.Views)
				{
					if(newline)
						writer.WriteLine();
					else
						newline = true;
					ScriptViewHeader(writer, view);
				}
			}
			AddScriptFile(fileName);
		}

		void ScriptViews(Database database)
		{
			foreach(var schema in database.Schemas)
				ScriptViews(schema);
		}

		void ScriptViews(Schema schema)
		{
			// Skip the schema if it does not have any views
			// (don't create an empty Views directory).
			if(!schema.Views.Any())
				return;

			string relativeDir = Path.Combine("Schemas", schema.Name, "Views");
			string dir = Path.Combine(OutputDirectory, relativeDir);
			if(!Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			foreach(var view in schema.Views)
			{
				string fileName = Path.Combine(relativeDir, view.Name + ".sql");
				string outputFileName = Path.Combine(OutputDirectory, fileName);
				AddScriptFile(fileName);
				OnProgressMessageReceived(fileName);

				using(var writer = new StreamWriter(outputFileName, false, this.Encoding))
				{
					ScriptView(writer, view);
				}
			}
		}

		void VerifyProperties()
		{
			if(OutputDirectory == null)
				OutputDirectory = String.Empty;
			if(Encoding == null)
				Encoding = new UTF8Encoding(false);
		}

		void WriteDateLiteral(TextWriter writer, DateTime dateTime)
		{
			writer.Write('\'');
			writer.Write(dateTime.ToString("yyyy-MM-dd"));
			writer.Write('\'');
		}

		void WriteLiteral(TextWriter writer, object value, NpgsqlDbType type)
		{
			if(value == null || value == DBNull.Value)
			{
				writer.Write("NULL");
				return;
			}
			switch(type)
			{
				case NpgsqlDbType.Bigint:
				case NpgsqlDbType.Boolean:
				case NpgsqlDbType.Double:
				case NpgsqlDbType.Integer:
				case NpgsqlDbType.Numeric:
				case NpgsqlDbType.Real:
				case NpgsqlDbType.Smallint:
					writer.Write(value);
					break;
				case NpgsqlDbType.Char:
					WriteStringLiteral(writer, value.ToString());
					break;
				case NpgsqlDbType.Date:
					WriteDateLiteral(writer, (DateTime)value);
					break;
				case NpgsqlDbType.Text:
				case NpgsqlDbType.Varchar:
					WriteStringLiteral(writer, (string)value);
					break;
				case NpgsqlDbType.Timestamp:
					WriteTimestampLiteral(writer, (DateTime)value);
					break;
				default:
					throw new ArgumentException("Unsupported type: " + type.ToString(), "type");
			}
		}

		void WriteRange(TextWriter writer, IEnumerable<string> values, string delimiter = null)
		{
			string innerDelimiter = null;
			foreach(string value in values)
			{
				if(innerDelimiter == null)
					innerDelimiter = delimiter;
				else
					writer.Write(innerDelimiter);
				writer.Write(value);
			}
		}

		void WriteStringLiteral(TextWriter writer, string value)
		{
			writer.Write('\'');
			writer.Write(value.Replace("'", "''").Replace(@"\", @"\\"));
			writer.Write('\'');
		}

		void WriteTimestampLiteral(TextWriter writer, DateTime dateTime)
		{
			writer.Write('\'');
			writer.Write(dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
			writer.Write('\'');
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
