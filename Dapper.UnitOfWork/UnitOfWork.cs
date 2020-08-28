using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using Dapper.UnitOfWork.Commands;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.UnitOfWork
{
    public class UnitOfWork : IUnitOfWork, IDisposable
    {
        private bool _disposed = false;
        private IDbConnection Connection { get; set; }
        private IDbTransaction Transaction { get; set; }
        private ICollection<CommandItem> CommandsList { get; set; }



        public UnitOfWork(IDbConnection connection)
        {
            Connection = connection;
            CommandsList = new List<CommandItem>();

            if (this.Connection.State == ConnectionState.Closed)
                this.Connection.Open();

        }


        public virtual void BeginTransaction()
            => this.BeginTransaction(IsolationLevel.ReadCommitted);

        public virtual void BeginTransaction(IsolationLevel il)
        {
            //Before to open Transaction, first is verified if there are some pending(s) command(s)
            //to be saved, if any, first of all the unit of work will save all pending commands, 
            //and only after that, the transaction will be starting
            if (this.Transaction == null && this.CommandsList.Count() > 0)
                this.SaveChanges();

            if (this.Transaction == null)
                this.Transaction = Connection.BeginTransaction(il);
        }

        public virtual void Commit()
        {
            if (this.Transaction != null)
                this.Transaction.Commit();
        }

        public virtual void RollBack()
        {
            if (this.Transaction != null)
                this.Transaction.Rollback();
        }



        public virtual int AddCommand(string sql)
        {
            this.CommandsList.Add(new CommandItem(Connection, sql));

            // if != null Transaction opened and managed by caller object
            if (this.Transaction != null)
                return ApplyChanges(this.Transaction);
            else
                return 0;
        }

        public virtual int AddCommand(string sql, object param)
        {
            this.CommandsList.Add(new CommandItem(Connection, sql, param));

            // if != null Transaction opened and managed by caller object
            if (this.Transaction != null)
                return ApplyChanges(this.Transaction);
            else
                return 0;
        }

        public virtual int AddCommand(string sql, object param, object relatedEntiy, string fieldSetGeneratedId)
        {
            this.CommandsList.Add(new CommandItem(Connection, sql, param, relatedEntiy, fieldSetGeneratedId));

            // if != null Transaction opened and managed by caller object
            if (this.Transaction != null)
                return ApplyChanges(this.Transaction);
            else
                return 0;
        }


        public virtual int SaveChanges()
        {
            if (this.CommandsList.Count > 1 && this.Transaction == null)
                return ApplyChangesWithTransaction();
            else
                return ApplyChanges();
        }


        public IDataReader ExecuteReader(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteReader(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<IDataReader> ExecuteReaderAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteReaderAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public object ExecuteScalar(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteScalar(sql, param, this.Transaction, commandTimeout, commandType);

        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteScalar<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteScalarAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<object> ExecuteScalarAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.ExecuteScalarAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public IEnumerable<object> Query(Type type, string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query(type, sql, param, this.Transaction, buffered, commandTimeout, commandType);

        public IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<T>(sql, param, this.Transaction, buffered, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TReturn>(sql, types, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>
                (sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);
        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TThird, TFourth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public IEnumerable<dynamic> Query(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query(sql, param, this.Transaction, buffered, commandTimeout, commandType);

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.Query<TFirst, TSecond, TThird, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TThird, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);


        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);


        public Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(sql, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<TReturn>> QueryAsync<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync<TReturn>(sql, types, map, param, this.Transaction, buffered, splitOn, commandTimeout, commandType);

        public Task<IEnumerable<object>> QueryAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryAsync(type, sql, param, this.Transaction, commandTimeout, commandType);

        public object QueryFirst(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirst(type, sql, param, this.Transaction, commandTimeout, commandType);

        public T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirst<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public dynamic QueryFirst(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirst(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<dynamic> QueryFirstAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<T> QueryFirstAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<object> QueryFirstAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstAsync(type, sql, param, this.Transaction, commandTimeout, commandType);

        public T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefault<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefault(sql, param, this.Transaction, commandTimeout, commandType);

        public object QueryFirstOrDefault(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefault(type, sql, param, this.Transaction, commandTimeout, commandType);

        public Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefaultAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefaultAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<object> QueryFirstOrDefaultAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryFirstOrDefaultAsync(type, sql, param, this.Transaction, commandTimeout, commandType);

        public SqlMapper.GridReader QueryMultiple(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryMultiple(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<SqlMapper.GridReader> QueryMultipleAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QueryMultipleAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public object QuerySingle(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingle(type, sql, param, this.Transaction, commandTimeout, commandType);

        public T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingle<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public dynamic QuerySingle(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingle(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<dynamic> QuerySingleAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<object> QuerySingleAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleAsync(type, sql, param, this.Transaction, commandTimeout, commandType);

        public Task<T> QuerySingleAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public dynamic QuerySingleOrDefault(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefault(sql, param, this.Transaction, commandTimeout, commandType);

        public T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefault<T>(sql, param, this.Transaction, commandTimeout, commandType);

        public object QuerySingleOrDefault(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefault(type, sql, param, this.Transaction, commandTimeout, commandType);
        public Task<object> QuerySingleOrDefaultAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefaultAsync(type, sql, param, this.Transaction, commandTimeout, commandType);

        public Task<dynamic> QuerySingleOrDefaultAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefaultAsync(sql, param, this.Transaction, commandTimeout, commandType);

        public Task<T> QuerySingleOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
            => this.Connection.QuerySingleOrDefaultAsync<T>(sql, param, this.Transaction, commandTimeout, commandType);



        public void Dispose() => Dispose(true);
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                this.Connection?.Close();
                this.Connection?.Dispose();
                this.Connection = null;

                this.CommandsList?.Clear();
                this.CommandsList = null;

                this.Transaction?.Dispose();
                this.Transaction = null;

                GC.SuppressFinalize(this);
            }

            _disposed = true;
        }


        private int ApplyChangesWithTransaction()
        {
            using (var _transaction = Connection.BeginTransaction())
            {
                var _qtChanges = ApplyChanges(_transaction);

                _transaction.Commit();

                return _qtChanges;
            }
        }
        private int ApplyChanges(IDbTransaction transaction = null)
        {
            int _qtRet = 0;

            foreach (var _commandItem in this.CommandsList)
                _qtRet = _qtRet + _commandItem.Execute(_commandItem.Sql, _commandItem.SqlParameters, transaction);

            this.CommandsList.Clear();

            return _qtRet;
        }

    }
}
