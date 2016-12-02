package org.caesar.hbase.example;

import org.apache.commons.codec.language.bm.Lang;
import org.caesar.hbase.annotation.HbaseDocument;
import org.caesar.hbase.annotation.HbaseField;
import org.caesar.hbase.annotation.HbaseRowKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by caesar.zhu on 15-11-4.
 */
//定义表名和列簇名（目前只支持单个表对应一个列簇）
@HbaseDocument(table = "my-table-user",family = "col_0")
public class UserModel {
    @HbaseRowKey
    @HbaseField
    private Long userId;
    @HbaseField
    private String userName;
    @HbaseField
    private Integer age;
    @HbaseField
    private Map<String,Integer> scoresMap;
    @HbaseField
    private List<String> hobiesList;
    @HbaseField
    private Double schooling;
    @HbaseField
    private Set<Integer> years;
    @HbaseField
    private Boolean end;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Map<String, Integer> getScoresMap() {
        return scoresMap;
    }

    public void setScoresMap(Map<String, Integer> scoresMap) {
        this.scoresMap = scoresMap;
    }

    public List<String> getHobiesList() {
        return hobiesList;
    }

    public void setHobiesList(List<String> hobiesList) {
        this.hobiesList = hobiesList;
    }

    public Double getSchooling() {
        return schooling;
    }

    public void setSchooling(Double schooling) {
        this.schooling = schooling;
    }

    public Set<Integer> getYears() {
        return years;
    }

    public void setYears(Set<Integer> years) {
        this.years = years;
    }

    public Boolean getEnd() {
        return end;
    }

    public void setEnd(Boolean end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "UserModel{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", age=" + age +
                ", scoresMap=" + scoresMap +
                ", hobiesList=" + hobiesList +
                ", schooling=" + schooling +
                ", years=" + years +
                ", end=" + end +
                '}';
    }
}
