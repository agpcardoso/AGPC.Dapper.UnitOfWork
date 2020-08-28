using Dapper;
using System.Data;
namespace Dapper.UnitOfWork.Commands
{
    public class CommandItem
    {
        public string Sql { get; private set; }
        public object SqlParameters { get; private set; }
        public object RelatedEntity { get; private set; }
        public string FieldNameSetGeneratedId { get; private set; }



        private IDbConnection Connection { get; }

        public CommandItem(IDbConnection connection, string sql)
        {
            this.Connection = connection;
            this.Sql = sql;
        }

        public CommandItem(IDbConnection connection, string sql, object param)
        {
            this.Connection = connection;
            this.Sql = sql;
            this.SqlParameters = param;
        }


        /// <summary>
        /// Just if you want return some generated id by database after an insert like an Sql Server identity code.
        /// The generated id will be returned by reference in object passed in relatedEntity parameter
        /// </summary>
        public CommandItem(IDbConnection connection, string sql, object param, object relatedEntity, string fieldNameSetGeneratedId)
        {
            this.Sql = sql;
            this.SqlParameters = param;
            this.Connection = connection;
            this.RelatedEntity = relatedEntity;
            this.FieldNameSetGeneratedId = fieldNameSetGeneratedId;
        }



        public int Execute(string sql, object param = null, IDbTransaction transaction = null)
        {
            if (this.RelatedEntity == null || FieldNameSetGeneratedId == string.Empty)
                return Connection.Execute(sql, param, transaction);
            else
            {
                var _result = Connection.ExecuteScalar<int>(sql, param, transaction);

                this.RelatedEntity.GetType().GetProperty(this.FieldNameSetGeneratedId).SetValue(this.RelatedEntity, _result);

                return (_result > 0 ? 1 : 0);
            }
        }

    }
}
