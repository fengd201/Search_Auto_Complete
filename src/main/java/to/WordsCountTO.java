package to;

public class WordsCountTO implements Comparable<WordsCountTO> {
    private int count;
    private String word;

    public WordsCountTO(int count, String word) {
        this.count = count;
        this.word = word;
    }

    public int compareTo(WordsCountTO o) {
        return new Integer(this.count).compareTo(new Integer(o.count));
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
