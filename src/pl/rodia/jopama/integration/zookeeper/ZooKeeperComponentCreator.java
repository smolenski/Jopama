package pl.rodia.jopama.integration.zookeeper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;

import pl.rodia.jopama.data.Component;
import pl.rodia.mpf.Task;

public class ZooKeeperComponentCreator extends ZooKeeperActorBase
{

	public ZooKeeperComponentCreator(
			String id,
			String addresses,
			Integer clusterSize,
			Long firstComponentId,
			Long numComponents
	)
	{
		super(
				id,
				addresses,
				clusterSize
		);
		this.createdComponents = new HashSet<ZooKeeperObjectId>();
		this.numOutstanding = new Long(0);
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.done = new Boolean(
				false
		);
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(
				3000
		);
	}

	@Override
	public void tryToPerform()
	{
		logger.info("tryToPerform");
		if (this.numOutstanding.compareTo(new Long(0)) > 0)
		{
			logger.info("tryToPerform, not performing because there are outstanding operations, numOutstanding: " + this.numOutstanding);
			return;
		}
		for (long i = this.firstComponentId; i < this.firstComponentId + this.numComponents; ++i)
		{
			assert i <= Integer.MAX_VALUE;
			Integer id = new Integer((int) i);
			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
					ZooKeeperObjectId.getComponentUniqueName(i)
			);
			ZooKeeperProvider zooKeeperProvider = this.zooKeeperMultiProvider.getResponsibleProvider(
					zooKeeperObjectId.getClusterId(
							this.zooKeeperMultiProvider.getNumClusters()
					)
			);
			synchronized (zooKeeperProvider)
			{
				if (
					zooKeeperProvider.zooKeeper == null
							||
							zooKeeperProvider.zooKeeper.getState() != States.CONNECTED
				)
				{
					continue;
				}
				else
				{
					Component component = new Component(new Integer(0), null, new Integer(id), null);
					byte[] serializedComponent = ZooKeeperHelpers.serializeComponent(component);
					this.numOutstanding += 1;
					zooKeeperProvider.zooKeeper.create(
							ZooKeeperHelpers.getComponentPath(zooKeeperObjectId),
							serializedComponent,
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT,
							new StringCallback()
							{
								@Override
								public void processResult(
										int rc, String path, Object ctx, String name
								)
								{
									logger.debug(
											"ZooKeeperCompoentCreator create file process result, name: " + zooKeeperObjectId.uniqueName + " rc: " + rc
									);
									schedule(
											new Task()
											{

												@Override
												public void execute()
												{
													fileCreationFinished(
															zooKeeperObjectId,
															rc == KeeperException.Code.OK.intValue()
													);
												}
											}
									);
								}
							},
							zooKeeperObjectId
					);
				}
			}
		}
	}

	void fileCreationFinished(
			ZooKeeperObjectId zooKeeperObjectId,
			Boolean success
	)
	{
		this.numOutstanding -= 1;
		if (
			success.equals(
					new Boolean(
							true
					)
			)
		)
		{
			this.createdComponents.add(zooKeeperObjectId);
			if (
				this.createdComponents.size() == this.numComponents
			)
			{
				synchronized (this)
				{
					this.done = new Boolean(
							true
					);
				}
			}
		}
	}

	void waitUntilDone() throws InterruptedException
	{
		synchronized (this)
		{
			while (
				this.done.equals(
						new Boolean(
								false
						)
				)
			)
			{
				this.wait(
						new Long(
								1000
						)
				);
			}
		}
	}

	Set<ZooKeeperObjectId> createdComponents;
	Long numOutstanding;
	Long firstComponentId;
	Long numComponents;
	Boolean done;

	public static void main(
			String[] args
	)
	{
		assert (args.length == 5);
		String id = args[0];
		String addresses = args[1];
		Integer clusterSize = Integer.parseInt(
				args[2]
		);
		Long firstComponentId = Long.parseLong(
				args[3]
		);
		Long numComponents = Long.parseLong(
				args[4]
		);
		ThreadExceptionHandlerSetter.setHandler();
		ZooKeeperComponentCreator componentCreator = new ZooKeeperComponentCreator(
				id,
				addresses,
				clusterSize,
				firstComponentId,
				numComponents
		);
		componentCreator.start();
		try
		{
			componentCreator.waitUntilDone();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		try
		{
			componentCreator.finish();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			e.printStackTrace();
		}
	}
}
