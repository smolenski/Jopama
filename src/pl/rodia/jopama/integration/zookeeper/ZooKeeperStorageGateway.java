package pl.rodia.jopama.integration.zookeeper;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class ZooKeeperStorageGateway extends RemoteStorageGateway
{

	public ZooKeeperStorageGateway(
			String name, String connectionString
	)
	{
		this.zooKeeperProvider = new ZooKeeperProvider(
				name,
				connectionString
		);
	}

	public void start()
	{
		this.zooKeeperProvider.start();
	}

	public void finish() throws InterruptedException
	{
		this.zooKeeperProvider.finish();
	}

	@Override
	public void requestTransaction(
			Integer transactionId, NewTransactionVersionFeedback feedback
	)
	{
		synchronized (this.zooKeeperProvider)
		{
			this.zooKeeperProvider.zooKeeper.getData(
					ZooKeeperHelpers.getTransactionPath(
							transactionId
					),
					null,
					new DataCallback()
					{

						@Override
						public void processResult(
								int rc, String path, Object ctx, byte[] data, Stat stat
						)
						{
							if (rc == KeeperException.Code.OK.intValue())
							{
								feedback.success(ZooKeeperHelpers.deserializeTransaction(data));
							}
							else if (rc == KeeperException.Code.NONODE.intValue())
							{
								feedback.success(null);
							}
						}
					},
					null
			);
		}
	}

	@Override
	public void requestComponent(
			Integer componentId, NewComponentVersionFeedback feedback
	)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		// TODO Auto-generated method stub

	}

	ZooKeeperProvider zooKeeperProvider;

}
