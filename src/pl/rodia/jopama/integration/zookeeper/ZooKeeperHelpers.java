package pl.rodia.jopama.integration.zookeeper;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
	static List<String> prepareZooKeeperClustersConnectionStrings(String addresses, Integer zooKeeperClusterSize)
	{
		List<String> splittedAddresses = Arrays.asList(addresses.split(","));
		assert (splittedAddresses.size() % zooKeeperClusterSize) == 0;
		Integer numZooKeeperClusters = splittedAddresses.size() / zooKeeperClusterSize;
		assert numZooKeeperClusters > 0;
		List<String> result = new LinkedList<String>();
		for (int i = 0; i < numZooKeeperClusters; ++i)
		{
			Integer splittedAddressesClusterOffset = i * zooKeeperClusterSize;
			StringBuilder zooKeeperConnectionString = new StringBuilder();
			for (int j = 0; j < zooKeeperClusterSize; ++j)
			{
				if (j != 0)
				{
					zooKeeperConnectionString.append(",");
				}
				zooKeeperConnectionString.append(splittedAddresses.get(splittedAddressesClusterOffset + j));
			}
			result.add(zooKeeperConnectionString.toString());
		}
		return result;
	}
}
