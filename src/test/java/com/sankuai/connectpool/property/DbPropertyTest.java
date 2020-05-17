package com.sankuai.connectpool.property;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DbPropertyTest {

    @Test
    public void getUserName() {
        Assert.assertEquals("root", Configuration.getUserName());
    }
}