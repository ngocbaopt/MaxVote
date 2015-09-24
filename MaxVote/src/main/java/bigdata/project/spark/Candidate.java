package bigdata.project.spark;

public class Candidate {

	String name;
	
	int vote;
	
	public Candidate(String name, int vote) {
		this.name = name;
		this.vote = vote;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setVote(int vote) {
		this.vote = vote;
	}
	
	public String getName() {
		return this.name;
	}
	
	public int getVote() {
		return this.vote;
	}
}
