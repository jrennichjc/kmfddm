package dynamo

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jessepeterson/kmfddm/ddm"
	"github.com/jessepeterson/kmfddm/storage"
)

type dsDeclarationStatus struct {
	PK           string `dynamodbav:"pk"`
	SK           string `dynamodbav:"sk"`
	Time         string `dynamodbav:"statusTime"`
	Identifier   string `dynamodbav:"statusID"`
	Active       bool   `dynamodbav:"statusActive"`
	Valid        string `dynamodbav:"statusValid"`
	ServerToken  string `dynamodbav:"server-token"`
	ManifestType string `dynamodbav:"statusType,omitempty"`
	ReasonsJSON  string `dynamodbav:"statusResons,omitempty"`
}

type dsDecError struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
	Time string `dynamodbav:"statusTime"`
	Path string `dynamodbav:"path"`
	Body string `dynamodbav:"body"`
}

func (s *DSDynamoTable) StoreDeclarationStatus(_ context.Context, enrollmentID string, status *ddm.StatusReport) error {

	// store raw report

	rawReport := dsGenericItem{
		PK:   "enroll#" + enrollmentID,
		SK:   "status#last",
		Body: string(status.Raw),
	}

	err := s.AddItem(rawReport)

	if err != nil {
		return err
	}

	if err = s.storeStatusDeclarations(enrollmentID, status.Declarations); err != nil {
		return fmt.Errorf("storing declaration status: %w", err)
	}

	// get status items, to add any new ones to them

	if err = s.storeStatusValues(enrollmentID, status.Values); err != nil {
		return fmt.Errorf("storing status values: %w", err)
	}

	if err = s.storeStatusErrors(enrollmentID, status.Errors); err != nil {
		return fmt.Errorf("storing status errors: %w", err)
	}

	return err
}

func (s *DSDynamoTable) RetrieveDeclarationStatus(_ context.Context, enrollmentIDs []string) (map[string][]ddm.DeclarationQueryStatus, error) {

	ret := make(map[string][]ddm.DeclarationQueryStatus)
	for _, enrollmentID := range enrollmentIDs {
		var decs []string
		var items []dsDecError
		var ddmErrors []storage.StatusError
		manifestMap := make(map[string]ddm.DeclarationQueryStatus)

		sets, _ := s.RetrieveEnrollmentSets(context.TODO(), enrollmentID)

		for _, set := range sets {
			setDecs, _ := s.RetrieveSetDeclarations(context.TODO(), set)
			decs = append(decs, setDecs...)
		}

		for _, dec := range decs {

			var item dsDeclarationStatus
			err := s.GetSingleItemPKSK("enroll#"+enrollmentID, "status#dec#"+dec, &item)

			if err != nil {
				return nil, err
			}

			var ddmError interface{}
			if len([]byte(item.ReasonsJSON)) > 0 {
				if err = json.Unmarshal([]byte(item.ReasonsJSON), &ddmError); err != nil {
					return nil, fmt.Errorf("unmarshal reason json: %w", err)
				}
			}

			// decode the timestamp
			var ts time.Time
			if err = ts.UnmarshalText([]byte(item.Time)); err != nil {
				return nil, fmt.Errorf("unmarshal time: %w", err)
			}

			manifestMap[dec] = ddm.DeclarationQueryStatus{
				DeclarationStatus: ddm.DeclarationStatus{
					Identifier:   dec,
					Active:       item.Active,
					Valid:        item.Valid,
					ServerToken:  item.ServerToken,
					ManifestType: item.ManifestType,
				},
				Reasons:        ddmError,
				StatusReceived: ts,
			}
		}

		for _, item := range items {

			var ts time.Time
			if err := ts.UnmarshalText([]byte(item.Time)); err != nil {
				return nil, fmt.Errorf("unmarshal time: %w", err)
			}

			ddmError := storage.StatusError{
				Path:      item.Path,
				Error:     item.Body,
				Timestamp: ts,
			}
			ddmErrors = append(ddmErrors, ddmError)
		}

		// turn back into a list
		ddmStatuses := make([]ddm.DeclarationQueryStatus, 0, len(manifestMap))
		for k := range manifestMap {
			ddmStatuses = append(ddmStatuses, manifestMap[k])
		}
		ret[enrollmentID] = ddmStatuses
	}
	return ret, nil
}

func (s *DSDynamoTable) RetrieveStatusErrors(_ context.Context, enrollmentIDs []string, offset, limit int) (map[string][]storage.StatusError, error) {

	ret := make(map[string][]storage.StatusError)

	for _, enrollmentID := range enrollmentIDs {

		var decs []string
		var items []dsDecError
		var ddmErrors []storage.StatusError

		sets, _ := s.RetrieveEnrollmentSets(context.TODO(), enrollmentID)

		for _, set := range sets {
			setDecs, _ := s.RetrieveSetDeclarations(context.TODO(), set)
			decs = append(decs, setDecs...)
		}

		for _, dec := range decs {

			di := new(ddm.DeclarationItems)

			var item dsDeclarationStatus
			_ = s.GetSingleItemPKSK("enroll#"+enrollmentID, "dec#status#"+dec, item)

			if err := json.Unmarshal([]byte(item.ReasonsJSON), &di); err != nil {
				return nil, fmt.Errorf("decoding declaration items json: %w", err)
			}

			if di == nil || (len(di.Declarations.Activations) < 1 && len(di.Declarations.Assets) < 1 && len(di.Declarations.Configurations) < 1 && len(di.Declarations.Management) < 1) {
				// no declarations or empty response; move on.
				continue
			}
		}

		err := s.GetItemsSKBeginsWith("enroll#"+enrollmentID, "status#error#", &items)
		if err != nil {
			return nil, err
		}

		for _, item := range items {

			var ts time.Time
			if err = ts.UnmarshalText([]byte(item.Time)); err != nil {
				return nil, fmt.Errorf("unmarshal time: %w", err)
			}

			ddmError := storage.StatusError{
				Path:      item.Path,
				Error:     item.Body,
				Timestamp: ts,
			}
			ddmErrors = append(ddmErrors, ddmError)
		}

		ret[enrollmentID] = ddmErrors
	}

	return ret, nil
}

func (s *DSDynamoTable) RetrieveStatusValues(_ context.Context, enrollmentIDs []string, pathPrefix string) (map[string][]storage.StatusValue, error) {

	ret := make(map[string][]storage.StatusValue)
	for _, enrollmentID := range enrollmentIDs {
		values, err := s.readStatusValues(enrollmentID)
		if err != nil {
			return nil, fmt.Errorf("reading status values: %w", err)
		}
		if pathPrefix != "" {
			values = filterPathPrefix(values, pathPrefix)
		}
		var sValues []storage.StatusValue
		for _, v := range values {
			sValues = append(sValues, storage.StatusValue{
				Path:  v.Path,
				Value: string(v.Value),
			})
		}
		if len(sValues) > 0 {
			ret[enrollmentID] = sValues
		}
	}
	return ret, nil
}

// helper functions

const (
	csvFilenameErrors       = "status.errors"
	csvFilenameDeclarations = "status.declarations"
	csvFilenameValues       = "status.values"
)

func containsValue(values []ddm.StatusValue, v ddm.StatusValue) int {
	for i, value := range values {
		if v.Path == value.Path && v.ValueType == value.ValueType && v.ContainerType == value.ContainerType && bytes.Equal(v.Value, value.Value) {
			return i
		}
	}
	return -1
}

func mergeStatusValues(dst, src []ddm.StatusValue) (ret []ddm.StatusValue) {
	ret = append([]ddm.StatusValue{}, dst...)
	for _, srcValue := range src {
		if containsValue(ret, srcValue) < 0 {
			ret = append(ret, srcValue)
		}
	}
	return
}

func (s *DSDynamoTable) readStatusValues(enrollmentID string) ([]ddm.StatusValue, error) {

	var item dsGenericItem

	err := s.GetSingleItemPKSK("enroll#"+enrollmentID, "status#values", &item)

	if err != nil {
		return nil, err
	}

	stringReader := strings.NewReader(item.Body)
	reader := csv.NewReader(stringReader)

	var ret []ddm.StatusValue
	for {
		// read a record
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading CSV record: %w", err)

		}

		// record is a set length
		if len(record) != 4 {
			return nil, fmt.Errorf("record fields: %d", len(record))
		}

		ret = append(ret, ddm.StatusValue{
			Path:          record[0],
			ContainerType: record[1],
			ValueType:     record[2],
			Value:         []byte(record[3]),
		})
	}

	return ret, nil
}

func (s *DSDynamoTable) storeStatusValues(enrollmentID string, values []ddm.StatusValue) error {
	if len(values) < 1 {
		return nil
	}

	curValues, err := s.readStatusValues(enrollmentID)
	if err != nil {
		return fmt.Errorf("reading values: %w", err)
	}

	// merge in the new values
	values = mergeStatusValues(curValues, values) // TODO: curValues, values

	if len(values) < 1 {
		// nothing to save
		return nil
	}
	b := new(bytes.Buffer)
	writer := csv.NewWriter(b)

	var records [][]string
	for _, v := range values {
		records = append(records, []string{
			v.Path,
			v.ContainerType,
			v.ValueType,
			string(v.Value),
		})
	}

	writer.WriteAll(records)

	item := dsGenericItem{
		PK:   "enroll#" + enrollmentID,
		SK:   "status#values",
		Body: b.String(),
	}

	err = s.AddItem(item)

	return err
}

func (s *DSDynamoTable) storeStatusDeclarations(enrollmentID string, declarations []ddm.DeclarationStatus) error {

	var err error

	for _, dec := range declarations {

		now := time.Now()
		nowText, err := now.MarshalText()
		if err != nil {
			return fmt.Errorf("marshal time to text: %w", err)
		}

		item := dsDeclarationStatus{
			PK:           "enroll#" + enrollmentID,
			SK:           "status#dec#" + dec.Identifier,
			Time:         string(nowText),
			Identifier:   dec.Identifier,
			Active:       dec.Active,
			Valid:        dec.Valid,
			ServerToken:  dec.ServerToken,
			ManifestType: dec.ManifestType,
			ReasonsJSON:  string(dec.ReasonsJSON),
		}

		err = s.AddItem(item)

		if err != nil {
			return err
		}
	}

	return err
}

func (s *DSDynamoTable) storeStatusErrors(enrollmentID string, ddmErrors []ddm.StatusError) error {

	var err error

	for _, statusError := range ddmErrors {

		now := time.Now()
		nowText, err := now.MarshalText()
		if err != nil {
			return fmt.Errorf("marshal time to text: %w", err)
		}

		item := dsDecError{
			PK:   "enroll#" + enrollmentID,
			SK:   "status#error#" + statusError.Path + "#" + string(nowText),
			Path: statusError.Path,
			Time: string(nowText),
			Body: string(statusError.ErrorJSON),
		}

		err = s.AddItem(item)

		if err != nil {
			return err
		}
	}

	return err
}

func filterPathPrefix(values []ddm.StatusValue, pathPrefix string) (ret []ddm.StatusValue) {
	for _, v := range values {
		var found bool
		if hasPre, hasSuf := strings.HasPrefix(pathPrefix, "%"), strings.HasSuffix(pathPrefix, "%"); len(v.Path) >= 3 && hasPre && hasSuf {
			found = strings.Contains(v.Path, pathPrefix[1:len(pathPrefix)-1])
		} else if hasPre {
			found = strings.HasSuffix(v.Path, pathPrefix[1:])
		} else if hasSuf {
			found = strings.HasPrefix(v.Path, pathPrefix[:len(pathPrefix)-1])
		} else {
			found = v.Path == pathPrefix
		}
		if found {
			ret = append(ret, v)
		}
	}
	return
}
