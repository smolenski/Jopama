package pl.rodia.jopama.data;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class SerializationUnitTests
{

	@Test
	public void serializationAndDeserializationShouldPreserveComponentEquality() throws ClassNotFoundException, IOException
	{
		Component componentOrig = new Component(
				new Integer(
						50
				),
				new SimpleObjectId(
						9
				),
				new Integer(
						5
				),
				new Integer(
						6
				)
		);
		Component componentCopy = Serializer.deserializeComponent(
				Serializer.serializeComponent(
						componentOrig
				)
		);
		assertThat(
				componentOrig,
				equalTo(
						componentCopy
				)
		);
	}

	@Test
	public void serializationAndDeserializationShouldPreserveTransactionEquality() throws ClassNotFoundException, IOException
	{
		TreeMap<ObjectId, TransactionComponent> transactionComponents = new TreeMap<ObjectId, TransactionComponent>();
		transactionComponents.put(
				new SimpleObjectId(
						101
				),
				new TransactionComponent(
						new Integer(
								50
						),
						ComponentPhase.NOT_LOCKED
				)
		);
		Transaction transactionOrig = new Transaction(
				TransactionPhase.LOCKING,
				transactionComponents,
				new Increment()
		);
		Transaction transactionCopy = Serializer.deserializeTransaction(
				Serializer.serializeTransaction(
						transactionOrig
				)
		);
		assertThat(
				transactionOrig,
				equalTo(
						transactionCopy
				)
		);
	}

	@Test
	public void serializedFunctionShouldWorkAsExpected() throws ClassNotFoundException, IOException
	{
		Function inc = new Increment();
		Function resInc = (Function) Serializer.deserializeObject(
				Serializer.serializeObject(
						inc
				)
		);
		ObjectId componentId = new SimpleObjectId(
				5
		);
		Integer origValue = new Integer(
				101
		);
		Map<ObjectId, Integer> arguments = new TreeMap<ObjectId, Integer>();
		arguments.put(
				componentId,
				origValue
		);
		Map<ObjectId, Integer> incResult = resInc.execute(
				arguments
		);
		assertThat(
				incResult.get(
						componentId
				),
				equalTo(
						new Integer(
								origValue + 1
						)
				)
		);
	}

	static final Logger logger = LogManager.getLogger();
}
