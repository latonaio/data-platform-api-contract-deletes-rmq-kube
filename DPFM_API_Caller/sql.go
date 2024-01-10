package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-contract-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-contract-deletes-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Contract = %d ", input.Header.Contract)
	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s \n AND IsMarkedForDeletion = %t", where, *input.Header.IsMarkedForDeletion)
	}
	where = fmt.Sprintf("%s \n AND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			header.Contract
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_contract_header_data as header ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ItemDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Item {
	where := fmt.Sprintf("WHERE header.Contract IS NOT NULL\nAND header.Contract = %d", input.Header.Contract)
	where := fmt.Sprintf("WHERE item.ContractItem IS NOT NULL\nAND item.ContractItem = %d", input.Item.ContractItem)
	// where = fmt.Sprintf("%s\nAND ( item.ItemDeliveryStatus, item.IsDdeleted, item.IsMarkedForDeletion) = ('NP', false, false) ", where)
	rows, err := c.db.Query(
		`SELECT 
			item.Contract, item.ContractItem
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_contract_item_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_contract_header_data as header
		ON header.Contract = item.Contract ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToItem(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ItemsDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Item {
	where := fmt.Sprintf("WHERE item.Contract IS NOT NULL\nAND header.Contract = %d", input.Header.Contract)
	//	where = fmt.Sprintf("%s\nAND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	// where = fmt.Sprintf("%s\nAND ( item.ItemDeliveryStatus, item.IsDdeleted, item.IsMarkedForDeletion) = ('NP', false, false) ", where)
	rows, err := c.db.Query(
		`SELECT 
			item.Contract, item.ContractItem
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_contract_item_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_contract_header_data as header
		ON header.Contract = item.Contract ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToItem(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
