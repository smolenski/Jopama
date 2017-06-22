package pl.rodia.jopama.integration.zookeeper;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import pl.rodia.jopama.data.Component;
import pl.rodia.mpf.Task;

public class ZooKeeperTestVerifier extends ZooKeeperActorBase
{

	public ZooKeeperTestVerifier(
			String id, String addresses, Integer clusterSize, Long firstComponentId, Long numComponents
	)
	{
		super(
				id,
				addresses,
				clusterSize
		);
		this.firstComponentId = firstComponentId;
		this.numComponents = numComponents;
		this.done = new Boolean(false);
		this.idsToRead = new TreeSet<Long>();
		for (Long cid = this.firstComponentId; cid < firstComponentId + this.numComponents; ++cid)
		{
			idsToRead.add(
					cid
			);
		}
		this.mapping = new TreeMap<Long, Long>();
	}

	@Override
	public Long getRetryDelay()
	{
		return new Long(
				2000
		);
	}

	@Override
	public void tryToPerform()
	{
		for (Long cid : this.idsToRead)
		{
			assert cid <= Integer.MAX_VALUE;
			Integer id = cid.intValue();
			ZooKeeperObjectId zooKeeperObjectId = new ZooKeeperObjectId(
					ZooKeeperObjectId.getComponentUniqueName(
							cid
					)
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
					zooKeeperProvider.zooKeeper.getData(
							ZooKeeperHelpers.getComponentPath(
									zooKeeperObjectId
							),
							null,
							new DataCallback()
							{
								@Override
								public void processResult(
										int rc, String path, Object ctx, byte[] data, Stat stat
								)
								{
									assert(rc != KeeperException.Code.NONODE.intValue());
									if (
										rc == KeeperException.Code.OK.intValue()
									)
									{
										ZooKeeperObjectId objectId = ZooKeeperHelpers.getIdFromPath(
												path
										);
										Long key = objectId.getId();
										Component component = ZooKeeperHelpers.deserializeComponent(
												data
										);
										Long value = new Long(
												component.value
										);
										schedule(
												new Task()
												{
													@Override
													public void execute()
													{
														onComponentValueRetrieved(key, value);
													}
											}
										);
									}
								}
							},
							null
					);
				}
			}
		}
	}
	
	void onComponentValueRetrieved(Long key, Long value)
	{
		this.mapping.put(key, value);
		this.idsToRead.remove(key);
		if (this.idsToRead.isEmpty())
		{
			assert (this.mapping.size() == this.numComponents);
			this.printState();
			this.verifyState();
			synchronized (this)
			{
				this.done = new Boolean(true);
			}
		}
	}
	
	void printState()
	{
		logger.info("MAPPING BEGIN");
		for (Map.Entry<Long, Long> entry : this.mapping.entrySet())
		{
			logger.info(entry.getKey() + "  => " + entry.getValue());
		}		
		logger.info("MAPPING END");
	}
	
	void verifyState()
	{
		Set<Long> values = new TreeSet<Long>();
		for (Map.Entry<Long, Long> entry : this.mapping.entrySet())
		{
			Long value = entry.getValue();
			if (value < this.firstComponentId || value >= (this.firstComponentId + this.numComponents))
			{
				throw new IllegalStateException("Unexpected value, value: " + value);
			}
			values.add(value);
		}
		if (!this.numComponents.equals(new Long(values.size())))
		{
			throw new IllegalStateException("Unexpected number of values, number: " + values.size());
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

	Long firstComponentId;
	Long numComponents;
	Boolean done;
	Set<Long> idsToRead;
	Map<Long, Long> mapping;

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
		ZooKeeperTestVerifier testVerifier = new ZooKeeperTestVerifier(
				id,
				addresses,
				clusterSize,
				firstComponentId,
				numComponents
		);
		testVerifier.start();
		try
		{
			testVerifier.waitUntilDone();
		}
		catch (InterruptedException e)
		{
			assert (false);
			e.printStackTrace();
		}
		try
		{
			testVerifier.finish();
		}
		catch (InterruptedException e)
		{
			assert (false);
			e.printStackTrace();
		}
		catch (ExecutionException e)
		{
			assert (false);
			e.printStackTrace();
		}
	}

}
