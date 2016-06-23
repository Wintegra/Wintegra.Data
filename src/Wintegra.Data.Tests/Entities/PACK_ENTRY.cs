using System;
using System.Collections.Generic;

namespace Wintegra.Data.Tests.Entities
{
	public class PACK_ENTRY
	{
		public string ID { get; set; }
		public DateTime Income { get; set; }

		public long? CloseDate { get; set; }
		public long CategoryCode { get; set; }
		public string CategoryName { get; set; }

		public long GuCode { get; set; }
		public long No { get; set; } 
	}

	public class PACK_ENTRY_WITH_FILES : PACK_ENTRY
	{
		public List<FILE_ENTRY> FILES { get; set; }
	}

	public class PACK_ENTRY_LIST : ENTRY_LIST<PACK_ENTRY>
	{
		public PACK_ENTRY_LIST() : base() { }
		public PACK_ENTRY_LIST(int capacity) : base(capacity) { }
	}
}