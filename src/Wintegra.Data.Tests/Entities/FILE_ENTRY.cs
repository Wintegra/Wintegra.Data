using System;

namespace Wintegra.Data.Tests.Entities
{
	public class FILE_ENTRY
	{
		public string ID { get; set; }
		public DateTime Income { get; set; }

		public string PACK_ID { get; set; }
		public long No { get; set; }

		public string FName { get; set; }
		public string LName { get; set; }
		public string MName { get; set; }

		public string BDate { get; set; }

		public string SNILS { get; set; }

		public string Address { get; set; }

		public long? CloseDate { get; set; }
		public long CategoryCode { get; set; }
		public string CategoryName { get; set; }  
	}
}