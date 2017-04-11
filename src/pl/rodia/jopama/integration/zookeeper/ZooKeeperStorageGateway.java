package pl.rodia.jopama.integration.zookeeper;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.ComponentChange;
import pl.rodia.jopama.data.ExtendedComponent;
import pl.rodia.jopama.data.ExtendedTransaction;
import pl.rodia.jopama.data.ObjectId;
import pl.rodia.jopama.data.TransactionChange;
import pl.rodia.jopama.gateway.ErrorCode;
import pl.rodia.jopama.gateway.NewComponentVersionFeedback;
import pl.rodia.jopama.gateway.NewTransactionVersionFeedback;
import pl.rodia.jopama.gateway.RemoteStorageGateway;

public class ZooKeeperStorageGateway extends RemoteStorageGateway
{

	public ZooKeeperStorageGateway(
			String addresses,
			Integer clusterSize
	)
	{
		this.zooKeeperMultiProvider = new ZooKeeperMultiProvider(
				addresses,
				clusterSize
		);
	}

	public void start()
	{
		this.zooKeeperMultiProvider.start();
	}

	public void finish() throws InterruptedException
	{
		this.zooKeeperMultiProvider.finish();
	}

	@Override
	public void requestTransaction(
			ObjectId transactionId, NewTransactionVersionFeedback feedback
	)
	{
		ZooKeeperObjectId objectId = (ZooKeeperObjectId) transactionId;
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
				objectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters())
		);
		synchronized (zooKeeperProvider)
		{
			if (
				zooKeeperProvider == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			zooKeeperProvider.zooKeeper.getData(
					ZooKeeperHelpers.getTransactionPath(
							objectId
					),
					false,
					new DataCallback()
					{

						@Override
						public void processResult(
								int rc, String path, Object ctx, byte[] data, Stat stat
						)
						{
							if (
								rc == KeeperException.Code.OK.intValue()
							)
							{
								feedback.success(
										new ExtendedTransaction(
												ZooKeeperHelpers.deserializeTransaction(
														data
												),
												stat.getVersion()
										)
								);
							}
							else if (
								rc == KeeperException.Code.NONODE.intValue()
							)
							{
								feedback.success(
										null
								);
							}
						}
					},
					null
			);
		}
	}

	@Override
	public void requestComponent(
			ObjectId componentId, NewComponentVersionFeedback feedback
	)
	{
		ZooKeeperObjectId objectId = (ZooKeeperObjectId) componentId;
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
				objectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters())
		);
		synchronized (zooKeeperProvider)
		{
			if (
				zooKeeperProvider == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			zooKeeperProvider.zooKeeper.getData(
					ZooKeeperHelpers.getComponentPath(
							objectId
					),
					false,
					new DataCallback()
					{

						@Override
						public void processResult(
								int rc, String path, Object ctx, byte[] data, Stat stat
						)
						{
							if (
								rc == KeeperException.Code.OK.intValue()
							)
							{
								feedback.success(
										new ExtendedComponent(
												ZooKeeperHelpers.deserializeComponent(
														data
												),
												stat.getVersion()
										)
								);
							}
							else if (
								rc == KeeperException.Code.NONODE.intValue()
							)
							{
								feedback.failure(
										ErrorCode.NOT_EXISTS
								);
							}
						}
					},
					null
			);
		}
	}

	@Override
	public void changeTransaction(
			TransactionChange transactionChange, NewTransactionVersionFeedback feedback
	)
	{
		ZooKeeperObjectId objectId = (ZooKeeperObjectId) transactionChange.transactionId;
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
				objectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters())
		);
		synchronized (zooKeeperProvider)
		{
			if (
				zooKeeperProvider == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			zooKeeperProvider.zooKeeper.setData(
					ZooKeeperHelpers.getTransactionPath(
							objectId
					),
					ZooKeeperHelpers.serializeTransaction(
							transactionChange.nextVersion
					),
					transactionChange.currentVersion.externalVersion,
					new StatCallback()
					{

						@Override
						public void processResult(
								int rc, String path, Object ctx, Stat stat
						)
						{
							if (
								rc == KeeperException.Code.OK.intValue()
							)
							{
								assert stat.getVersion() == (transactionChange.currentVersion.externalVersion + 1);
								feedback.success(
										new ExtendedTransaction(
												transactionChange.nextVersion,
												stat.getVersion()
										)
								);
							}
							else if (
								rc == KeeperException.Code.NONODE.intValue()
							)
							{
								feedback.failure(
										ErrorCode.NOT_EXISTS
								);
							}
							else if (
								rc == KeeperException.Code.BADVERSION.intValue()
							)
							{
								feedback.failure(
										ErrorCode.BASE_VERSION_NOT_EQUAL
								);
							}
						}
					},
					null
			);
		}
	}

	@Override
	public void changeComponent(
			ComponentChange componentChange, NewComponentVersionFeedback feedback
	)
	{
		ZooKeeperObjectId objectId = (ZooKeeperObjectId) componentChange.componentId;
		ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
				objectId.getClusterId(this.zooKeeperMultiProvider.getNumClusters())
		);
		synchronized (zooKeeperProvider)
		{
			if (
				zooKeeperProvider == null
						||
						zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
			)
			{
				return;
			}
			zooKeeperProvider.zooKeeper.setData(
					ZooKeeperHelpers.getComponentPath(
							objectId
					),
					ZooKeeperHelpers.serializeComponent(
							componentChange.nextVersion
					),
					componentChange.currentVersion.externalVersion,
					new StatCallback()
					{

						@Override
						public void processResult(
								int rc, String path, Object ctx, Stat stat
						)
						{
							if (
								rc == KeeperException.Code.OK.intValue()
							)
							{
								assert stat.getVersion() == (componentChange.currentVersion.externalVersion.intValue() + 1);
								feedback.success(
										new ExtendedComponent(
												componentChange.nextVersion,
												stat.getVersion()
										)
								);
							}
							else if (
								rc == KeeperException.Code.NONODE.intValue()
							)
							{
								feedback.failure(
										ErrorCode.NOT_EXISTS
								);
							}
							else if (
								rc == KeeperException.Code.BADVERSION.intValue()
							)
							{
								feedback.failure(
										ErrorCode.BASE_VERSION_NOT_EQUAL
								);
							}
						}
					},
					null
			);
		}
	}

	ZooKeeperMultiProvider zooKeeperMultiProvider;

}
