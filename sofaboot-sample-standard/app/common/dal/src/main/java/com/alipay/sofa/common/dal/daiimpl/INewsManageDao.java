/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.common.dal.daiimpl;

import com.alipay.sofa.common.dal.dai.NewsManageDao;
import com.alipay.sofa.common.dal.dao.NewsDO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * @author qilong.zql
 * @since 2.5.0
 */
public class INewsManageDao implements NewsManageDao {

    @Autowired
    private DataSource dataSource;

    @Override
    public int insert(NewsDO newDO) throws SQLException {
        Assert.notNull(newDO, "newDo must not be null");
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        return statement.executeUpdate(String.format(
            "INSERT INTO NewsTable (AUTHOR, TITLE) VALUES('%s', '%s');", newDO.getAuthor(),
            newDO.getTitle()));
    }

    @Override
    public List<NewsDO> query(String author) throws SQLException {
        Assert.hasText(author, "author must has text");
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(String.format(
            "SELECT * FROM NewsTable WHERE AUTHOR='%s';", author));
        List<NewsDO> answer = new LinkedList<NewsDO>();
        while (resultSet.next()) {
            NewsDO newDO = new NewsDO();
            newDO.setAuthor(resultSet.getString(2));
            newDO.setTitle(resultSet.getString(3));
            answer.add(newDO);
        }
        return answer;
    }

    @Override
    public void delete(String author) throws SQLException {
        Assert.hasText(author, "author must has text");
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(String.format("DELETE FROM NewsTable WHERE AUTHOR='%s';", author));
    }
}