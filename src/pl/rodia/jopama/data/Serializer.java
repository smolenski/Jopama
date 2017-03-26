package pl.rodia.jopama.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class Serializer
{
	static byte[] serializeObject(
			Object object
	) throws IOException
	{
		try (
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutput out = new ObjectOutputStream(
						bos
				)
		)
		{
			out.writeObject(
					object
			);
			return bos.toByteArray();
		}
	}

	static Object deserializeObject(
			byte[] data
	) throws IOException, ClassNotFoundException
	{
		try (
				ByteArrayInputStream bis = new ByteArrayInputStream(
						data
				);
				ObjectInput in = new ObjectInputStream(
						bis
				)
		)
		{
			return in.readObject();
		}
	}

	public static byte[] serializeComponent(
			Component component
	) throws IOException
	{
		return serializeObject(
				component
		);
	}

	public static byte[] serializeTransaction(
			Transaction transaction
	) throws IOException
	{
		return serializeObject(
				transaction
		);
	}

	public static Component deserializeComponent(
			byte[] data
	) throws IOException, ClassNotFoundException
	{
		return (Component) deserializeObject(
				data
		);
	}

	public static Transaction deserializeTransaction(
			byte[] data
	) throws IOException, ClassNotFoundException
	{
		return (Transaction) deserializeObject(
				data
		);
	}
}
