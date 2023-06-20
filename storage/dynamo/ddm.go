package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jessepeterson/kmfddm/ddm"
)

// RetrieveEnrollmentDeclarationJSON retrieves the DDM declaration JSON for an enrollment ID.
func (s *DSDynamoTable) RetrieveEnrollmentDeclarationJSON(_ context.Context, declarationID, declarationType, enrollmentID string) ([]byte, error) {

	var item dsDeclaration

	err := s.GetSingleItemPKSK("dec#"+declarationID, "dec#", &item)

	if err != nil {
		return nil, err
	}

	return []byte(item.JSON), err
}

// RetrieveDeclarationItemsJSON retrieves the DDM declaration-items JSON for an enrollment ID.
func (s *DSDynamoTable) RetrieveDeclarationItemsJSON(_ context.Context, enrollmentID string) ([]byte, error) {
	var item dsGenericItem

	err := s.GetSingleItemPKSK("enroll#"+enrollmentID, "json", &item)
	if err != nil {
		return nil, err
	}

	return []byte(item.Body), err
}

// RetrieveDeclarationItemsJSON retrieves the DDM token JSON for an enrollment ID.
func (s *DSDynamoTable) RetrieveTokensJSON(_ context.Context, enrollmentID string) ([]byte, error) {
	var item dsGenericItem

	err := s.GetSingleItemPKSK("enroll#"+enrollmentID, "tokens", &item)
	if err != nil {
		return nil, err
	}

	return []byte(item.Body), err
}

// writeDeclarationDDM looks up the enrollments associated with a declaration and writes the DDM files for each.
func (s *DSDynamoTable) writeDeclarationDDM(declarationID string) error {
	// first find all enrollment IDs mapped to this declaration.
	declarationIDs, err := s.declarationEnrollmentIDs(declarationID)
	if err != nil {
		return err
	}
	for _, id := range declarationIDs {
		// write the enrollment DDM files
		if err = s.writeEnrollmentDDM(id); err != nil {
			return err
		}
	}
	return nil
}

// writeSetDDM writes the DDM files for all enrollments belonging to a set.
func (s *DSDynamoTable) writeSetDDM(setName string) error {

	var items []dsGenericItem
	err := s.GetItemsSKBeginsWith("set#"+setName, "enroll#", &items)

	if err != nil {
		return err
	}

	for _, item := range items {
		if err = s.writeEnrollmentDDM(strings.TrimPrefix(item.SK, "enroll#")); err != nil {
			return err
		}
	}
	return nil
}

// writeEnrollmentDDM generates all enrollment ID-specific DDM declarations.
func (s *DSDynamoTable) writeEnrollmentDDM(enrollmentID string) error {

	var sets []dsGenericItem
	var enrollmentSets []string

	err := s.GetItemsSKBeginsWith("enroll#"+enrollmentID, "set#", &sets)

	if err != nil {
		return err
	}

	for _, set := range sets {
		enrollmentSets = append(enrollmentSets, strings.TrimPrefix(set.SK, "set#"))
	}

	enrollmentDeclarations := make(map[string]struct{})

	for _, setName := range enrollmentSets {

		var declarations []dsDeclaration

		// get all the declarations for this set
		err = s.GetItemsSKBeginsWith("set#"+setName, "dec#", &declarations)

		if err != nil {
			return err
		}

		for _, declarationID := range declarations {
			// collect declaration IDs in our map
			enrollmentDeclarations[strings.TrimPrefix(declarationID.SK, "dec#")] = struct{}{}
		}
	}

	// create our token and declaration-items builders
	di := ddm.NewDIBuilder(s.newHash)
	ti := ddm.NewTokensBuilder(s.newHash)

	for declarationID := range enrollmentDeclarations {

		dec, err := s.RetrieveDeclaration(context.TODO(), declarationID)

		// read and parse declaration
		d, err := ddm.ParseDeclaration(dec.Raw)
		if err != nil {
			return fmt.Errorf("parsing declaration: %w", err)
		}

		// add to our DI and tokens builders
		di.AddDeclarationData(d)
		ti.AddDeclarationData(d)
	}

	// finalize the builders
	di.Finalize()
	ti.Finalize()

	// marshal and write the declarations-items JSON
	diJSON, err := json.Marshal(&di.DeclarationItems)
	if err != nil {
		return err
	}

	jsonItem := dsGenericItem{
		PK:   "enroll#" + enrollmentID,
		SK:   "json",
		Body: string(diJSON),
	}

	err = s.AddItem(jsonItem)

	if err != nil {
		return err
	}

	// marshal and write the tokens JSON
	tiJSON, err := json.Marshal(&ti.TokensResponse)
	if err != nil {
		return err
	}

	tokenItem := dsGenericItem{
		PK:   "enroll#" + enrollmentID,
		SK:   "tokens",
		Body: string(tiJSON),
	}

	err = s.AddItem(tokenItem)

	if err != nil {
		return err
	}

	return nil
}
