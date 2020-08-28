using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using static Dapper.SqlMapper;

namespace Dapper.UnitOfWork
{ 
     public interface IUnitOfWork
    {

        void BeginTransaction();
        void BeginTransaction(IsolationLevel il);
        void Commit();
        void RollBack();

        int AddCommand(string sql);
        int AddCommand(string sql, object param);
        int AddCommand(string sql, object param, object relatedEntiy, string fieldNameSetGeneratedId);

        int SaveChanges();



        IDataReader ExecuteReader(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<IDataReader> ExecuteReaderAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        object ExecuteScalar(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<object> ExecuteScalarAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<object> Query(Type type, string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<dynamic> Query(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<TReturn>> QueryAsync<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null);

        Task<IEnumerable<object>> QueryAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        object QueryFirst(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        dynamic QueryFirst(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<dynamic> QueryFirstAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<T> QueryFirstAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<object> QueryFirstAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        object QueryFirstOrDefault(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<object> QueryFirstOrDefaultAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        GridReader QueryMultiple(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<GridReader> QueryMultipleAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        object QuerySingle(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        dynamic QuerySingle(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<dynamic> QuerySingleAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<object> QuerySingleAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<T> QuerySingleAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        dynamic QuerySingleOrDefault(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        object QuerySingleOrDefault(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<object> QuerySingleOrDefaultAsync(Type type, string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<dynamic> QuerySingleOrDefaultAsync(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        Task<T> QuerySingleOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

    }
}

