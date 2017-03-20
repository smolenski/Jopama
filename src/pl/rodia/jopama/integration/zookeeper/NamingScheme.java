package pl.rodia.jopama.integration.zookeeper;

public class NamingScheme
{
	static String getComponentPath(Integer componentId)
	{
		return "/Components/" + componentId.toString();
	}
	static String getTransactionPath(Integer transactionId)
	{
		return "/Transactions/" + transactionId.toString();
	}
}
