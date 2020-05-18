package com.sankuai.connectpool.property;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class NormalTest {

    Connection con = null;// 创建一个数据库连接
    PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    ResultSet result = null;// 创建一个结果集对象
    ResultSetMetaData metaData = null;//创建一个表头信息对象

    @Before
    public void before() throws Exception{
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://localhost:3306/test";//localhost 为本级地址，studata为数据库名
        String userName = "root";
        String password = "12345678";
        con = DriverManager.getConnection(url, userName, password);// 获取连接

        System.out.println("数据库连接成功！");

    }

    @After
    public void after() throws Exception{
        // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
        // 注意关闭的顺序，最后使用的最先关闭
        if (result != null)
            try {
                result.close();
                if (pre != null)
                    pre.close();
                if (con != null)
                    con.close();
                System.out.println("数据库连接已关闭！");
            } catch (SQLException e) {
                e.printStackTrace();
            }

    }

    @Test
    public void testPool() throws Exception{
        String sql = "select * from teacher where t_id=?";//预编译语句，?代表参数
        pre = con.prepareStatement(sql);// 实例化预编译语句
        pre.setInt(1, 123);// 设置参数，前面的1表示参数的索引，而不是表中列名的索引
        result = pre.executeQuery();// 执行查询，注意括号中不需要再加参数
        metaData  = result.getMetaData();//获取表头信息
        while (result.next()) {
            // 当结果集不为空时
            Assert.assertEquals("zhangchuyuan", result.getString("t_name"));
        }
    }
}