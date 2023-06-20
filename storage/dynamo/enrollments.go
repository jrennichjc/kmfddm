package dynamo

import (
	"context"
	"strings"
)

// RetrieveEnrollmentSets returns the slice of sets associated with an enrollment ID.
func (s *DSDynamoTable) RetrieveEnrollmentSets(_ context.Context, enrollmentID string) ([]string, error) {
	
	var sets []string
	var dsSets []dsGenericItem

	err := s.GetItemsSKBeginsWith("enroll#" + enrollmentID, "set#", &dsSets)

	if err != nil {
		for _, item := range dsSets {
			sets = append(sets, strings.TrimPrefix(item.SK, "set#"))
		}
	}

	return sets, err
}

// StoreEnrollmentSet creates the association between an enrollment and a set.
func (s *DSDynamoTable) StoreEnrollmentSet(_ context.Context, enrollmentID, setName string) (bool, error) {
	item := dsGenericItem{
		PK: "enroll#" + enrollmentID,
		SK: "set#" + setName,
	}

	err := s.AddItem(item)

	if err != nil {
		return false, err
	}

	err = s.writeEnrollmentDDM(enrollmentID)

	return true, err
}

// RemoveEnrollmentSet removes the association between an enrollment and a set.
func (s *DSDynamoTable) RemoveEnrollmentSet(_ context.Context, enrollmentID, setName string) (bool, error) {
	item := dsGenericItem{
		PK: "enroll#" + enrollmentID,
		SK: "set#" + setName,
	}

	change := s.deleteSingleItem(item)

	err := s.writeEnrollmentDDM(enrollmentID)

	return change, err
}

// declarationEnrollmentIDs finds all the enrollment IDs that are associated with a declaration.
func (s *DSDynamoTable) declarationEnrollmentIDs(declarationID string) ([]string, error) {

	var enrollments []string
	var items []dsGenericItem

	err := s.GetItemsSKBeginsWith("dec#" + declarationID, "set#", &items)

	if err != nil {
		return enrollments, err
	}

	for _, item := range items {

		items = nil

		err = s.GetReverseItems(item.PK, "enroll#", &items)

		if err == nil {
			for _, enroll := range items {
				enrollments = append(enrollments, strings.TrimPrefix(enroll.PK, "enroll#"))
			}
		}
	}
	return enrollments, nil
}

// RetrieveDeclarationEnrollmentIDs retrieves a list of enrollment IDs that are transitively associated with a declaration.
func (s *DSDynamoTable) RetrieveDeclarationEnrollmentIDs(_ context.Context, declarationID string) ([]string, error) {
	return s.declarationEnrollmentIDs(declarationID)
}

// RetrieveSetEnrollmentIDs retrieves a list of enrollment IDs that are associated with a set.
func (s *DSDynamoTable) RetrieveSetEnrollmentIDs(_ context.Context, setName string) ([]string, error) {
	
	var items []dsGenericItem
	var enrollments []string

	err := s.GetReverseItems("set#" + setName, "enroll#", &items)

	if err != nil {
		return nil, err
	}

	for _, item := range items {
		enrollments = append(enrollments, strings.TrimPrefix(item.PK, "enroll#"))
	}

	return enrollments, err
}
