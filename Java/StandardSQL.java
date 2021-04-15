package com.myapp.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardSQL {
	
	public static Connection getConnection() {
        Connection con = null;
    
        try {
			String userName =  "user";
			String pass = "password";
			String url = "<myconnectionstring>";
			
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(url, userName, pass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
	public static int insertOrUpdate(String query, List<String> parameters) throws SQLException
	{
		
		
		Connection con = getConnection();
		PreparedStatement p = null;
		int rowsEffected = 0;
		boolean isInsert = false;
		try {
			if(query.toLowerCase().trim().startsWith("insert"))
			{
				p = con.prepareStatement(query, p.RETURN_GENERATED_KEYS);
				isInsert = true;
			}
			else {
				p = con.prepareStatement(query);
			}
			
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rowsEffected = p.executeUpdate();
			if(rowsEffected>0 && isInsert)
			{
				ResultSet rs = null;
				rs = p.getGeneratedKeys();
				
				if(rs.next()) {
					rowsEffected = rs.getInt(1);
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(con,p,null);
		}
		return rowsEffected;
	}
	
	public static int insertOrUpdate(Connection con, String query, List<String> parameters) throws SQLException
	{
		
		PreparedStatement p = null;
		int rowsEffected = 0;
		boolean isInsert = false;
		try {
			if(query.toLowerCase().trim().startsWith("insert"))
			{
				p = con.prepareStatement(query, p.RETURN_GENERATED_KEYS);
				isInsert = true;
			}
			else {
				p = con.prepareStatement(query);
			}
			
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rowsEffected = p.executeUpdate();
			if(rowsEffected>0 && isInsert)
			{
				ResultSet rs = null;
				rs = p.getGeneratedKeys();
				
				if(rs.next()) {
					rowsEffected = rs.getInt(1);
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(null,p,null);
		}
		return rowsEffected;
	}
	
	
	public static List<Map<String, String>> selectQuery(String query, List<String> parameters) throws SQLException
	{
		Connection con = getConnection();
		PreparedStatement p = null;
		ResultSet rs = null;
		List<Map<String, String>> table = new ArrayList<Map<String, String>>();
		try  {
			p = con.prepareStatement(query);
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rs = p.executeQuery();
			List<String> colmuns = new ArrayList<String>();
			java.sql.ResultSetMetaData rsmd = rs.getMetaData();
			
			int columnCount = rsmd.getColumnCount();

			for (int i = 1; i <= columnCount; i++ ) {
				colmuns.add(rsmd.getColumnName(i));
			}
			
			while(rs.next())
			{
				Map<String, String> row = new HashMap<String, String>();
				for(String column : colmuns)
				{
					row.put(column, rs.getString(column));
				}
				table.add(row);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(con,p,rs);
		}
		return table;
	}
	
	public static List<Map<String, String>> selectQuery(Connection con, String query, List<String> parameters) throws SQLException
	{
		
		PreparedStatement p = null;
		ResultSet rs = null;
		List<Map<String, String>> table = new ArrayList<Map<String, String>>();
		try  {
			p = con.prepareStatement(query);
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rs = p.executeQuery();
			List<String> colmuns = new ArrayList<String>();
			java.sql.ResultSetMetaData rsmd = rs.getMetaData();
			
			int columnCount = rsmd.getColumnCount();

			for (int i = 1; i <= columnCount; i++ ) {
				colmuns.add(rsmd.getColumnName(i));
			}
			
			while(rs.next())
			{
				Map<String, String> dataPoint = new HashMap<String, String>();
				for(String column : colmuns)
				{
					dataPoint.put(column, rs.getString(column));
				}
				table.add(dataPoint);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(null,p,rs);
		}
		return table;
	}
	
	
	public static long selectCountQuery(String query, List<String> parameters) throws SQLException
	{
		Connection con = getConnection();
		PreparedStatement p = null;
		ResultSet rs = null;
		long count = 0;
		try {
			p = con.prepareStatement(query);
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rs = p.executeQuery();
			if(rs.next())
			{
				count = rs.getLong(1);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(con,p,rs);
		}
		return count;
	}
	
	public static long selectCountQuery(Connection con, String query, List<String> parameters) throws SQLException
	{
		
		PreparedStatement p = null;
		ResultSet rs = null;
		long count = 0;
		try {
			p = con.prepareStatement(query);
			if(parameters.size()>0)
			{
				int i = 1;
				for(String param : parameters)
				{
					p.setString(i, param);
					i++;
				}
			}
			rs = p.executeQuery();
			if(rs.next())
			{
				count = rs.getLong(1);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			close(null,p,rs);
		}
		return count;
	}
	
	public static void close(Connection c, PreparedStatement p, ResultSet r) throws SQLException
	{
		if(p!=null)
		{
			p.close();
		}
		if(r!=null)
		{
			r.close();
		}
		if(c!=null)
		{
			c.close();
		}
	}

}
