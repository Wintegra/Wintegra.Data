using System.Data;

namespace Wintegra.Data.Db2Client
{
	internal class Db2Command : IDbCommand
	{
		private readonly IDbCommand _command;
		private readonly IDataParameterCollection _parameters;

		public Db2Command(IDbCommand command)
		{
			_command = command;
			_parameters = new Db2DataParameterCollection(_command.Parameters);
		}

		public void Dispose()
		{
			_command.Dispose();
		}

		public void Prepare()
		{
			_command.Prepare();
		}

		public void Cancel()
		{
			_command.Cancel();
		}

		public IDbDataParameter CreateParameter()
		{
			return _command.CreateParameter();
		}

		public int ExecuteNonQuery()
		{
			return _command.ExecuteNonQuery();
		}

		public IDataReader ExecuteReader()
		{
			return new Db2DataReader(_command.ExecuteReader());
		}

		public IDataReader ExecuteReader(CommandBehavior behavior)
		{
			return new Db2DataReader(_command.ExecuteReader(behavior));
		}

		public object ExecuteScalar()
		{
			return _command.ExecuteScalar();
		}

		public IDbConnection Connection
		{
			get { return _command.Connection; }
			set { _command.Connection = value; }
		}

		public IDbTransaction Transaction
		{
			get { return _command.Transaction; }
			set { _command.Transaction = value; }
		}

		public string CommandText
		{
			get { return _command.CommandText; }
			set { _command.CommandText = Db2CommandText.ToODBC(value); }
		}

		public int CommandTimeout
		{
			get { return _command.CommandTimeout; }
			set { _command.CommandTimeout = value; }
		}

		public CommandType CommandType
		{
			get { return _command.CommandType; }
			set { _command.CommandType = value; }
		}

		public IDataParameterCollection Parameters
		{
			get { return _parameters; }
		}

		public UpdateRowSource UpdatedRowSource
		{
			get { return _command.UpdatedRowSource; }
			set { _command.UpdatedRowSource = value; }
		}
	}
}