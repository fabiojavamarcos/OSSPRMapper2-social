package DAO;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import model.Apriori;
import model.PrIssue;
import util.DBUtil;

public class FileDAO {
	private static FileDAO instancia;
	private String dbcon;
	private String user;
	private String pswd;

	private FileDAO(String dbcon, String user, String pswd) {
		this.dbcon = dbcon;
		this.user = user;
		this.pswd = pswd;
	}

	public static FileDAO getInstancia(String dbcon, String user, String pswd) {
		if (instancia == null) {
			instancia = new FileDAO(dbcon, user, pswd);
		}
		return instancia;
	}

	public ArrayList<String> buscaAPI(String pr, String java, String projectName) {
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		ArrayList<String> es = new ArrayList<String>();
		boolean found = false;

		try {
			Statement comandoSql = con.createStatement();

			String sql = "select expert from file a, \"file_API\" b, \"API_specific\" c where a.full_name = b.file_name and c.api_name_fk = b.api_name and a.project = '"
					+ projectName + "' and a.file_name like '%" + java + "%' and expert is not null GROUP BY c.expert";

			// System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);

			String expert = null;

			while (rs.next()) {
				expert = rs.getString("expert");
				// System.out.println("expert string: " + expert);

				if (expert != "Trash") {
					es.add(expert);
					/// System.out.println("expert array: " + es);
					// System.out.println("\n");
					found = true;
				}

			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		if (found)
			return es;
		else
			return null;

	}

	public boolean insertApriori(String pr, String java, String expert, String project) {
		// TODO Auto-generated method stub
		Connection con = DBUtil.getConnection(dbcon, user, pswd);

		try {
			Statement comandoSql = con.createStatement();

			String sql = "insert into apriori (pr,java,expert,project) values (" + pr + ",'" + java + "'" + ",'"
					+ expert + "', '" + project + "')";

			// System.out.println(sql);

			comandoSql.executeUpdate(sql);

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			return false;
		}
		return true;

	}

	public boolean insertPr(String pr, String title, String body, String project) {
		// TODO Auto-generated method stub
		Connection con = DBUtil.getConnection(dbcon, user, pswd);

		try {
			Statement comandoSql = con.createStatement();

			String sql = "insert into pr values (" + pr + ",'" + title + "'" + ",'" + body + "', '" + project + "')";

			System.out.println(sql);

			comandoSql.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// System.out.println(e.getMessage());
			return false;
		}
		return true;

	}

	public ArrayList getAprioris(String project) {
		// TODO Auto-generated method stub
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		ArrayList<Apriori> prs = new ArrayList<Apriori>();
		boolean found = false;
		try {
			Statement comandoSql = con.createStatement();

			// <<<<<<< HEAD
			String sql = "select pr, a.expert from apriori a where project = \'" + project
					+ "\' GROUP BY pr, a.expert ORDER BY pr";
			// ======= select pr, a.expert from apriori a where project = 'jabref' GROUP BY
			// pr, a.expert ORDER BY pr

			// String sql = "select general from file a, \"file_API\" b, \"API_specific\" c
			// where a.file_name = b.file_name and c.api_name_fk = b.api_name and
			// a.full_name like '%"+ java + "%' GROUP BY c.general";
			// String sql = "select pr, a.expert from apriori a GROUP BY pr, a.expert order
			// by pr";
			// >>>>>>> 18981556b016fe3128de310029f1f97ffb4ded49

			System.out.println(sql + "\n");

			ResultSet rs = comandoSql.executeQuery(sql);

			String expert = null;
			int pr = 0;

			while (rs.next()) {
				expert = rs.getString("expert");
				pr = rs.getInt("pr");
				Apriori ap = new Apriori();
				ap.setGeneral(expert); // using the old general field to hold experts
				ap.setPr(pr);
				prs.add(ap);
				found = true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			// System.out.println(e.getMessage());
		}
		if (found)
			return prs;
		else
			return null;

	}

	public ArrayList<String> getTitleBody(int pr, String project) {
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		String title = null;
		String body = null;
		ArrayList<String> result = new ArrayList();
		boolean found = false;
		try {
			Statement comandoSql = con.createStatement();

			// String sql = "select general from file a, \"file_API\" b, \"API_specific\" c
			// where a.file_name = b.file_name and c.api_name_fk = b.api_name and
			// a.full_name like '%"+ java + "%' GROUP BY c.general";
			String sql = "select title, body from pr where pr = " + pr + " and project = '" + project + "'";

			System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);

			if (rs.next()) {
				title = rs.getString("title");
				body = rs.getString("body");
				result.add(title);
				result.add(body);
				found = true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		if (found)
			return result;
		else
			return null;

	}

	public ArrayList<String> getDistinctGenerals() {
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		String general = null;
		String col = null;
		ArrayList<String> result = new ArrayList();
		boolean found = false;
		try {
			Statement comandoSql = con.createStatement();

			String sql = "select distinct expert from apriori ";

			System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();

			while (rs.next()) {
				col = rsmd.getColumnName(1);
				general = rs.getString(1);
				result.add(general);

				found = true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		if (found)
			return result;
		else
			return null;

	}

	public ArrayList<PrIssue> getIssues(int pr, String project) {
		// TODO Auto-generated method stub
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		String prRes = "";
		String issue = "";
		String issueTitle = "";
		String issueBody = "";
		String issueComments = "";
		String issueTitleLink = "";
		String issueBodyLink = "";
		String issueCommentsLink = "";
		int isPR = 0;
		int isTrain = 0;
		String commitMessage = "";
		String prComments = "";
		String col = null;
		ArrayList<PrIssue> result = new ArrayList();

		try {
			Statement comandoSql = con.createStatement();

			String sql = "select pr,issue,issue_title,issue_body,issue_comments,issue_title_linked,issue_body_linked,issue_comments_linked,is_train,commit_message,is_pr, pr_comments "
					+ "from pr_issue where pr = '" + pr + "' and project = '" + project + "'";

			System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();

			while (rs.next()) {
				col = rsmd.getColumnName(1);
				// general = rs.getString(1);
				prRes = rs.getString(1);
				issue = rs.getString(2);
				issueTitle = rs.getString(3);
				issueBody = rs.getString(4);
				issueComments = rs.getString(5);
				issueTitleLink = rs.getString(6);
				issueBodyLink = rs.getString(7);
				issueCommentsLink = rs.getString(8);

				isTrain = Integer.parseInt(rs.getString(9));
				commitMessage = rs.getString(10);
				isPR = Integer.parseInt(rs.getString(11));
				prComments = rs.getString(12);

				PrIssue pri = new PrIssue();
				pri.setCommitMessage(commitMessage);
				pri.setIsPR(isPR);
				pri.setIssue(issue);
				pri.setIssueBody(issueBody);
				pri.setIssueBodyLink(issueBodyLink);
				pri.setIssueComments(issueComments);
				pri.setIssueCommentsLink(issueCommentsLink);
				pri.setIssueTitle(issueTitle);
				pri.setIssueTitleLink(issueTitleLink);
				pri.setIsTrain(isTrain);
				pri.setPrComments(prComments);
				result.add(pri);

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		return result;

	}

	public ArrayList<Integer> getSocialInt(int pr, String project) {
		Connection con = DBUtil.getConnection(dbcon, user, pswd);
		int num_discussants = 0;
		int num_comments = 0;
		int wordiness = 0;
		int edges = 0;
		int vertices = 0;
		int diameter = 0;

		ArrayList<Integer> result = new ArrayList();
		boolean found = false;
		try {
			Statement comandoSql = con.createStatement();

			// String sql = "select general from file a, \"file_API\" b, \"API_specific\" c
			// where a.file_name = b.file_name and c.api_name_fk = b.api_name and
			// a.full_name like '%"+ java + "%' GROUP BY c.general";
			String sql = "select num_discussants, num_comments, wordiness, edges, vertices, diameter from pr_issue where pr = '"
					+ pr + "' and project = '" + project + "'";

			System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);

			if (rs.next()) {
				try {
					num_discussants = rs.getInt("num_discussants");
					num_comments = rs.getInt("num_comments");
					wordiness = rs.getInt("wordiness");
					edges = rs.getInt("edges");
					vertices = rs.getInt("vertices");
					diameter = rs.getInt("diameter");
				} catch (Exception ec) {
					System.out.println(ec.getMessage());
					System.out.println("PR: " + pr + " with nulls. Setting to 0");
					num_discussants = 0;
					num_comments = 0;
					wordiness = 0;
					edges = 0;
					vertices = 0;
					diameter = 0;
				}

				result.add(num_discussants);
				result.add(num_comments);
				result.add(wordiness);
				result.add(edges);
				result.add(vertices);
				result.add(diameter);

				found = true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		if (found)
			return result;
		else
			return null;

	}

	public ArrayList<Double> getSocialDouble(int pr, String project) {

		Connection con = DBUtil.getConnection(dbcon, user, pswd);

		double density = 0.0;
		double betweenness_avg = 0.0;
		double betweenness_max = 0.0;
		double betweenness_sum = 0.0;
		double closeness_avg = 0.0;
		double closeness_max = 0.0;
		double closeness_sum = 0.0;
		double constraint_avg = 0.0;
		double constraint_max = 0.0;
		double constraint_sum = 0.0;
		double effective_size_avg = 0.0;
		double effective_size_max = 0.0;
		double effective_size_sum = 0.0;
		double efficiency_avg = 0.0;
		double efficiency_max = 0.0;
		double efficiency_sum = 0.0;
		double hierarchy_avg = 0.0;
		double hierarchy_max = 0.0;
		double hierarchy_sum = 0.0;

		ArrayList<Double> result = new ArrayList();

		boolean found = false;

		try {
			Statement comandoSql = con.createStatement();

			// String sql = "select general from file a, \"file_API\" b, \"API_specific\" c
			// where a.file_name = b.file_name and c.api_name_fk = b.api_name and
			// a.full_name like '%"+ java + "%' GROUP BY c.general";
			String sql = "select density, betweenness_avg, betweenness_max, betweenness_sum, closeness_avg, closeness_max, closeness_sum, constraint_avg, constraint_max, constraint_sum, effective_size_avg, effective_size_max, effective_size_sum, efficiency_avg, efficiency_max, efficiency_sum, hierarchy_avg, hierarchy_max, hierarchy_sum from pr_issue where pr = '"
					+ pr + "' and project = '" + project + "'";

			System.out.println(sql);

			ResultSet rs = comandoSql.executeQuery(sql);

			if (rs.next()) {
				try {
					density = rs.getDouble("density");
					betweenness_avg = rs.getDouble("betweenness_avg");
					betweenness_max = rs.getDouble("betweenness_max");
					betweenness_sum = rs.getDouble("betweenness_sum");
					closeness_avg = rs.getDouble("closeness_avg");
					closeness_max = rs.getDouble("closeness_max");
					closeness_sum = rs.getDouble("closeness_sum");
					constraint_avg = rs.getDouble("constraint_avg");
					constraint_max = rs.getDouble("constraint_max");
					constraint_sum = rs.getDouble("constraint_sum");
					effective_size_avg = rs.getDouble("effective_size_avg");
					effective_size_max = rs.getDouble("effective_size_max");
					effective_size_sum = rs.getDouble("effective_size_sum");
					efficiency_avg = rs.getDouble("efficiency_avg");
					efficiency_max = rs.getDouble("efficiency_max");
					efficiency_sum = rs.getDouble("efficiency_sum");
					hierarchy_avg = rs.getDouble("hierarchy_avg");
					hierarchy_max = rs.getDouble("hierarchy_max");
					hierarchy_sum = rs.getDouble("hierarchy_sum");

				} catch (Exception ec) {
					System.out.println(ec.getMessage());
					System.out.println("PR: " + pr + " with nulls. Setting to 0");
					density = 0.0;
					betweenness_avg = 0.0;
					betweenness_max = 0.0;
					betweenness_sum = 0.0;
					closeness_avg = 0.0;
					closeness_max = 0.0;
					closeness_sum = 0.0;
					constraint_avg = 0.0;
					constraint_max = 0.0;
					constraint_sum = 0.0;
					effective_size_avg = 0.0;
					effective_size_max = 0.0;
					effective_size_sum = 0.0;
					efficiency_avg = 0.0;
					efficiency_max = 0.0;
					efficiency_sum = 0.0;
					hierarchy_avg = 0.0;
					hierarchy_max = 0.0;
					hierarchy_sum = 0.0;
				}

				result.add(density);
				result.add(betweenness_avg);
				result.add(betweenness_max);
				result.add(betweenness_sum);
				result.add(closeness_avg);
				result.add(closeness_max);
				result.add(closeness_sum);
				result.add(constraint_avg);
				result.add(constraint_max);
				result.add(constraint_sum);
				result.add(effective_size_avg);
				result.add(effective_size_max);
				result.add(effective_size_sum);
				result.add(efficiency_avg);
				result.add(efficiency_max);
				result.add(efficiency_sum);
				result.add(hierarchy_avg);
				result.add(hierarchy_max);
				result.add(hierarchy_sum);

				found = true;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		if (found)
			return result;
		else
			return null;

	}

}
