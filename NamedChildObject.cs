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

namespace Mercent.AWS.Redshift
{
	public class NamedChildObject
	{
		public string Description { get; set; }
		public string Name { get; set; }
		public NamedSchemaObject Parent { get; internal set; }

		public string GetQuotedName(QuoteMode mode = QuoteMode.WhenNecessary)
		{
			return RedshiftUtility.GetQuotedIdentifier(this.Name, mode);
		}

		public string GetQualifiedName(QuoteMode mode = QuoteMode.WhenNecessary)
		{
			if(this.Parent == null)
				return RedshiftUtility.GetQuotedIdentifier(this.Name, mode, false);
			else
				return this.Parent.GetQualifiedName(mode) + '.' + RedshiftUtility.GetQuotedIdentifier(this.Name, mode, true);
		}

		public override string ToString()
		{
			if(Parent == null)
				return this.Name;
			else
				return Parent.ToString() + '.' + this.Name;
		}
	}
}
