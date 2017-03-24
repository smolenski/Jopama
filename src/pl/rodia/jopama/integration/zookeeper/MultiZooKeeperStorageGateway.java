package pl.rodia.jopama.integration.zookeeper;

import java.util.LinkedList;
import java.util.List;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class MultiZooKeeperStorageGateway extends RemoteStorageGateway
{

	public MultiZooKeeperStorageGateway(String addresses, Integer zooKeeperClusterSize)
	{
		this.connectionStrings = ZooKeeperHelpers.prepareZooKeeperClustersConnectionStrings(addresses, zooKeeperClusterSize);
		this.clusterGateways = new LinkedList<ZooKeeperStorageGateway>();
		for (int i = 0; i < this.connectionStrings.size(); ++i)
		{
			this.clusterGateways.add(new ZooKeeperStorageGateway("ZooKeeperStorageGateway_" + i, this.connectionStrings.get(i)));
		}
	}
	
	public void start()
	{
		for (ZooKeeperStorageGateway zooKeeperStorageGateway : this.clusterGateways)
		{
			zooKeeperStorageGateway.start();
		}
	}
	
	public void finish() throws InterruptedException
	{
		for (ZooKeeperStorageGateway zooKeeperStorageGateway : this.clusterGateways)
		{
			zooKeeperStorageGateway.finish();
		}		
	}
	
	ZooKeeperStorageGateway getResponsibleGateway(Integer id)
	{
		return this.clusterGateways.get(id % this.clusterGateways.size());
	}
	
	@Override
	public void requestTransaction(
			Integer transactionId, NewTransactionVersionFeedback feedback
	)
	{
		this.getResponsibleGateway(transactionId).requestTransaction(transactionId, feedback);
	}

	@Override
	public void requestComponent(
			Integer componentId, NewComponentVersionFeedback feedback
	)
	{
		this.getResponsibleGateway(componentId).requestComponent(componentId, feedback);
	}

	@Override
	public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		this.getResponsibleGateway(transactionChange.transactionId).changeTransaction(transactionChange, feedback);
	}

	@Override
	public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		this.getResponsibleGateway(componentChange.componentId).changeComponent(componentChange, feedback);
	}

	final List<String> connectionStrings;
	final List<ZooKeeperStorageGateway> clusterGateways;
}
