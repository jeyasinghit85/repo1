

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.ResultSetDynaClass;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import com.app.core.datasource.DBUtil;
import com.app.core.datasource.DBUtilFactory;
import com.app.core.logger.AppLogger;

/**
 * @author jeyasingh
 * @version 1.0
 */
public class DbQueryUtil {

	/**
	 * Description : Default Constructor
	 */
	 static AppLogger applog = AppLogger.getInstance();
	public DbQueryUtil() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Description : Method used to fetch the select query results as List which
	 * contains the fethed column name as key and the result as value
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return result
	 * @throws SQLException
	 */
	public List<Map<String, Object>> getRowsAsMap( String query,
			Object[] param) throws SQLException {
		List<Map<String, Object>> result = null;
		Connection con =null;
		QueryRunner qr = new QueryRunner();
		try {
			con =getConnection();
			if (param != null) {
				result = qr.query(con, query, param, new MapListHandler());
			} else {
				result = qr.query(con, query, new MapListHandler());
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : Method used to get the select query result as Map, only one
	 * row can be fetched using this.
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return result
	 * @throws SQLException
	 */
	public Map<String, Object> getRowAsMap( String query,
			Object[] param) throws SQLException {
		List<Map<String, Object>> result = null;
		QueryRunner qr = new QueryRunner();
		Connection con =null;
		try {
			con =getConnection();
			if (param != null) {
				result = qr.query(con, query, param, new MapListHandler());
			} else {
				result = qr.query(con, query, new MapListHandler());
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		if (result.size() > 1) {
			throw new SQLException("too many records found");
		}
		if (result.size() == 0) {
			return new HashMap();
		}
		return result.get(0);
	}

	/**
	 * Description : Method used to get the select query results as java bean
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @param bean
	 * @return result
	 * @throws SQLException
	 */
	public List<Object> getRowsAsBean( String query,
			Object[] param, Class bean) throws SQLException {
		List<Object> result = null;
		QueryRunner qr = new QueryRunner();
		ResultSetHandler rsh = new BeanListHandler(bean);
		Connection con=null;
		try {
			con =getConnection();
			if (param != null) {
				result = (java.util.List<java.lang.Object>) qr.query(con,
						query, param, rsh);
			} else {
				result = (java.util.List<java.lang.Object>) qr.query(con,
						query, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : Method used to get the select query result as bean, only
	 * one row can be fetched using this.
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @param bean
	 * @return result
	 * @throws SQLException
	 */
	public Object getRowAsBean(String query, Object[] param,
			Class bean) throws SQLException {
		Object result = null;
		QueryRunner qr = new QueryRunner();
		ResultSetHandler rsh = new BeanHandler(bean);
		Connection con=null;
		try {
			con =getConnection();
			if (param != null) {
				result = qr.query(con, query, param, rsh);
			} else {
				result = qr.query(con, query, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : Method used to get the select query results as dyna bean.
	 * 
	 * @param con
	 * @param sql
	 * @param params
	 * @return result
	 */
	public List<Object> getRowsAsDynaBean( String sql,
			Object[] params) {
		ResultSetHandler<List<Object>> rsh = new ResultSetHandler<List<Object>>() {
			List<Object> beanList = new ArrayList<Object>();
			Object bean = null;

			// @SuppressWarnings("unchecked")
			public List<Object> handle(ResultSet rs) throws SQLException {
				if (rs == null) {
					return null;
				}
				try {
					ResultSetDynaClass dynaClass = new ResultSetDynaClass(rs);
					Iterator<DynaBean> rows = dynaClass.iterator();
					while (rows.hasNext()) {
						DynaBean dynaBean = (DynaBean) rows.next();
						beanList.add(dynaBean);
					}
				} catch (Exception e) {
					System.out.println("ResultSet convert to Bean Error!");
					e.printStackTrace();
				}
				return beanList;
			}
		};
		List<Object> result = null;
		QueryRunner qr = new QueryRunner();
		Connection con=null;
		try {
			con=getConnection();
			if (params != null) {
				result = qr.query(con, sql, params, rsh);
			} else {

				result = qr.query(con, sql, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + sql);
			System.err.println("param:" + parseParams(params));
			this.rollbackAndClose(con);
			ex.printStackTrace();
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : Method used to get the select query result as dyna bean,
	 * only one row can be fetched using this.
	 * 
	 * @param con
	 * @param sql
	 * @param params
	 * @return beanList
	 * @throws SQLException
	 */
	public Object getRowAsDynaBean( String sql, Object[] params)
			throws SQLException {
		ResultSetHandler<List<Object>> rsh = new ResultSetHandler<List<Object>>() {
			List<Object> beanList = new ArrayList<Object>();
			Object bean = null;

			@SuppressWarnings("unchecked")
			public List<Object> handle(ResultSet rs) throws SQLException {
				if (rs == null) {
					return null;
				}
				try {
					ResultSetDynaClass dynaClass = new ResultSetDynaClass(rs);
					Iterator<DynaBean> rows = dynaClass.iterator();
					while (rows.hasNext()) {
						DynaBean dynaBean = (DynaBean) rows.next();
						beanList.add(dynaBean);
					}
				} catch (Exception e) {
					System.out.println("ResultSet convert to Bean Error!");
					e.printStackTrace();
				}

				return beanList;
			}
		};
		List<Object> result = null;
		QueryRunner qr = new QueryRunner();
		Connection con=null;
		try {
			con=getConnection();
			if (params != null) {
				result = qr.query(con, sql, params, rsh);
			} else {
				result = qr.query(con, sql, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + sql);
			System.err.println("param:" + parseParams(params));
			this.rollbackAndClose(con);
			ex.printStackTrace();
		}finally{
			releaseConnection(con);
		}
		if (result.size() > 1)
			throw new SQLException("too many records found");
		return result.get(0);
	}

	/**
	 * Description : Method used to get the single column value for the given
	 * query
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @param columnIndex
	 * @return result
	 * @throws SQLException
	 */
	public Object getSingleValue( String query, Object[] param,
			int columnIndex) throws SQLException {
		Object result;
		ResultSetHandler rsh;
		QueryRunner qr = new QueryRunner();
		Connection con=null;
		if (columnIndex != 0) {
			rsh = new ScalarHandler(columnIndex);
		} else {
			rsh = new ScalarHandler();
		}
		try {
			con=getConnection();
			if (param != null) {
				result = qr.query(con, query, param, rsh);
			} else {
				result = qr.query(con, query, rsh);
			}
			return result;
		} catch (SQLException ex) {
			System.out.println("query:" + query);
			System.out.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
	}

	/**
	 * Description : Method used to perform update for the query argument
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return count
	 * @throws SQLException
	 */
	public int update( String query, Object[] param)
			throws SQLException {
		int count = 0;
		QueryRunner qr = new QueryRunner();
		Connection con=null;
		try {
			con=getConnection();
			if (param != null) {

				count = qr.update(con, query, param);
			} else {

				count = qr.update(con, query);
			}
			return count;
		} catch (SQLException ex) {
			System.out.println("query:" + query);
			System.out.println("param:" + parseParams(param));
			this.rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
	}

	/**
	 * Description : Method used to set the auto commit feature to true or false
	 * 
	 * @param con
	 * @param auto
	 * @throws SQLException
	 */
	public void setAutoCommit(Connection con, boolean auto) throws SQLException {
		try {
			if (con != null)
				con.setAutoCommit(auto);
		} catch (SQLException ex) {
			con.close();
			throw ex;
		}
	}

	/**
	 * Description : Method used to rollback the connection
	 * 
	 * @param con
	 * @throws SQLException
	 */
	public void rollback(Connection con) throws SQLException {
		try {
			if (con != null) {
				con.rollback();
			}
		} catch (SQLException ex) {
			con.close();
			throw ex;
		}
	}

	/**
	 * Description : Method used to commit the given connection
	 * 
	 * @param con
	 * @throws SQLException
	 */
	public void commit(Connection con) throws SQLException {
		try {
			if (con != null) {
				con.commit();
			}
		} catch (SQLException ex) {
			con.close();
			throw ex;
		}
	}

	/**
	 * Description : Method used to close the given connection
	 * 
	 * @param con
	 */
	public void close(Connection con) {
		try {
			if (con != null) {
				if (!con.isClosed()) {
					con.close();
				}
			}
			con = null;
		} catch (Exception ex) {
		}
	}

	/**
	 * Description : Method used to rollback and close the given connection
	 * 
	 * @param con
	 */
	private void rollbackAndClose(Connection con) {
		try {
			con.rollback();
			con.close();
		} catch (SQLException ex) {
		}
	}

	/**
	 * Description : Method used to group the input parameter. This is used for
	 * the logging purpose
	 * 
	 * @param params
	 * @return
	 */
	protected static String parseParams(Object[] params) {
		StringBuilder tmpBuf = new StringBuilder();

		if (params != null) {
			tmpBuf.append("[ ");
			for (int i = 0; i < params.length; i++) {
				if (params[i] == null) {
					tmpBuf.append("null");
				} else {
					tmpBuf.append(params[i].toString());
				}
				if ((i + 1) != params.length) {
					tmpBuf.append(", ");
				}
			}
			tmpBuf.append(" ]");
		}
		return tmpBuf.toString();
	}

	/**
	 * Description : Method used to fetch a row detail. Only one row can be
	 * retained using this.
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return result
	 * @throws SQLException
	 */
	public Object[] getRowAsArray(String query, Object[] param)
			throws SQLException {
		System.out.println("param" + param + "query" + query);
		Object[] result = null;
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Object[]> rsh = new ArrayHandler();
		Connection con=null;
		try {
			con=getConnection();
			if (param != null) {
				result = qr.query(con, query, param, rsh);
			} else {
				result = qr.query(con, query, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : Method used to fetch a row detail. Only one row can be
	 * retained using this.
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return result
	 * @throws SQLException
	 */
	public List<Object> getRowAsArrayList( String query,
			Object[] param) throws SQLException {
		System.out.println("param" + param + "query" + query);
		Object[] result = null;
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Object[]> rsh = new ArrayHandler();
		Connection con=null;
		try {
			con=getConnection();
			if (param != null) {
				result = qr.query(con, query, param, rsh);
			} else {
				result = qr.query(con, query, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return Arrays.asList(result);
	}

	/**
	 * Description : Method used to get the select query rows details as
	 * arraylist
	 * 
	 * @param con
	 * @param query
	 * @param param
	 * @return result
	 * @throws SQLException
	 */
	public List<Object[]> getRowsAsArrayList(String query,
			Object[] param) throws SQLException {
		List<Object[]> result = null;
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<List<Object[]>> rsh = new ArrayListHandler();
		Connection con=null;
		try {
			con=getConnection();
			if (param != null) {
				result = qr.query(con, query, param, rsh);
			} else {
				result = qr.query(con, query, rsh);
			}
		} catch (SQLException ex) {
			System.err.println("query:" + query);
			System.err.println("param:" + parseParams(param));
			rollbackAndClose(con);
			throw ex;
		}finally{
			releaseConnection(con);
		}
		return result;
	}

	/**
	 * Description : method used to get the query from the resource bundle
	 * 
	 * @param packageName
	 * @param process
	 * @param method
	 * @param queryId
	 * @return
	 */
	public String getQuery(String packageName, String methodName, String queryId) {
		String query = null;
		String bundleName = null;
		bundleName = packageName + "." + "query.query";
		ResourceBundle bundle = ResourceBundle.getBundle(bundleName);
		query = bundle.getString(methodName + "." + queryId);
		return query;
	}

	/**
	 * Description : method used to get the query from the resource bundle
	 * 
	 * @param packageName
	 * @param className
	 * @param methodName
	 * @return
	 */
	public String getQuery(String packageName, String methodName) {
		String query = null;
		String bundleName = null;
		bundleName = packageName + "." + "query.query";
		ResourceBundle bundle = ResourceBundle.getBundle(bundleName);
		query = bundle.getString(methodName);
		return query;
	}

	/**
	 * Description : method used to get the query of the enclosing method
	 * 
	 * @param stackTraceId
	 */
	public String getQuery(int traceId) {
		String query = null;
		String className = Thread.currentThread().getStackTrace()[traceId]
				.getClassName();
		String MethodName = className
				+ "."
				+ Thread.currentThread().getStackTrace()[traceId]
						.getMethodName();
		String PackName = className.substring(0, className.lastIndexOf("."));
		String bundleName = null;
		bundleName = PackName + "." + "query.query";
		ResourceBundle bundle = ResourceBundle.getBundle(bundleName);
		query = bundle.getString(MethodName);
		return query;
	}

	/**
	 * Description : method used to get the query of the enclosing method
	 */
	public String getEncMethodQuery() {
		String query = null;
		String className = Thread.currentThread().getStackTrace()[2]
				.getClassName();
		String MethodName = className + "."
				+ Thread.currentThread().getStackTrace()[2].getMethodName();
		String PackName = className.substring(0, className.lastIndexOf("."));
		String bundleName = null;
		bundleName = PackName + "." + "query.query";
		ResourceBundle bundle = ResourceBundle.getBundle(bundleName);
		query = bundle.getString(MethodName);
		return query;
	}

	/**
	 * Description : method used to get the query of the enclosing method
	 * 
	 * @param QueryId
	 */
	public String getEncMethodQuery(String queryId) {
		String query = null;
		String className = Thread.currentThread().getStackTrace()[2]
				.getClassName();
		String MethodName = className + "."
				+ Thread.currentThread().getStackTrace()[2].getMethodName();
		String PackName = className.substring(0, className.lastIndexOf("."));
		String bundleName = null;
		bundleName = PackName + "." + "query.query";
		ResourceBundle bundle = ResourceBundle.getBundle(bundleName);
		query = bundle.getString(MethodName + "." + queryId);
		return query;
	}

	public static String toString( Object o ) {
	    ArrayList<String> list = new ArrayList<String>();
	    DbQueryUtil.toString( o, o.getClass(), list );
	    return o.getClass().getName().concat( list.toString() );
	  }

	  private static void toString( Object o, Class<?> clazz, List<String> list ) {
	    Field f[] = clazz.getDeclaredFields();
	    AccessibleObject.setAccessible( f, true );
	    for ( int i = 0; i < f.length; i++ ) {
	      try {
	        list.add( f[i].getName() + "=" + f[i].get(o) );
	      }
	      catch ( IllegalAccessException e ) { e.printStackTrace(); }
	      }
	      if ( clazz.getSuperclass().getSuperclass() != null ) {
	         toString( o, clazz.getSuperclass(), list );
	      }
	  }
	  public Connection getConnection(){
			DBUtil dbUtil = null;
			Connection conn = null;
			try {
			dbUtil = DBUtilFactory.getInstance();
			conn = dbUtil.getConnection();			
	       } catch (Exception exe) {
				exe.printStackTrace();
				applog.appTraceLog("DbQueryUtil :getConnection Error in fetching Conncetion:::" + exe);
			} 
			return conn;
	  }
	  public void releaseConnection(Connection conn){
			DBUtil dbUtil = null;		  
			try {
				if (conn != null) {
					dbUtil = DBUtilFactory.getInstance();
					dbUtil.releaseConnection(conn);
				}	
	       } catch (Exception exe) {
				exe.printStackTrace();
				applog.appTraceLog("Error in releasing Connection :::" + exe);
	       }	  
	  }
}