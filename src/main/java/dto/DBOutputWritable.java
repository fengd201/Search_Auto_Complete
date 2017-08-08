package dto;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{
    private String startingPhrase;
    private String followingWord;
    private int count;

    public DBOutputWritable(String startingPhrase, String followingWord, int count) {
        this.startingPhrase = startingPhrase;
        this.followingWord = followingWord;
        this.count = count;
    }

    public String getStartingPhrase() {
        return startingPhrase;
    }

    public void setStartingPhrase(String startingPhrase) {
        this.startingPhrase = startingPhrase;
    }

    public String getFollowingWord() {
        return followingWord;
    }

    public void setFollowingWord(String followingWord) {
        this.followingWord = followingWord;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, startingPhrase);
        preparedStatement.setString(2, followingWord);
        preparedStatement.setInt(3, count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.startingPhrase = resultSet.getString(1);
        this.followingWord = resultSet.getString(2);
        this.count = resultSet.getInt(3);
    }
}
