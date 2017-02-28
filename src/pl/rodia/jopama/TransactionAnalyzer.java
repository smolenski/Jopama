package pl.rodia.jopama;

import pl.rodia.jopama.data.UnifiedAction;

public interface TransactionAnalyzer {

	UnifiedAction getChange(Integer transactionId);

}
