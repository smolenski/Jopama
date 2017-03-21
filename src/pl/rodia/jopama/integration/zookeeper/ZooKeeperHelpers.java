package pl.rodia.jopama.integration.zookeeper;

import pl.rodia.jopama.data.Component;
import pl.rodia.jopama.data.Transaction;

public class ZooKeeperHelpers
{
	static String getComponentPath(Integer componentId)
	{
		return "/Components/" + componentId.toString();
	}
	static String getTransactionPath(Integer transactionId)
	{
		return "/Transactions/" + transactionId.toString();
	}
	static byte [] serializeTransaction(Transaction transaction)
	{
		assert false;
		return null;
	}
	static Transaction deserializeTransaction(byte[] data)
	{
		assert false;
		return null;
	}
	static byte [] serializeComponent(Component component)
	{
		assert false;
		return null;
	}
	static Component deserializeComponent(byte[] data)
	{
		assert false;
		return null;
	}
}
