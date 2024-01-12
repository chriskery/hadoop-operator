// Copyright 2022 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"github.com/chriskery/hadoop-cluster-operator/pkg/apis/kubecluster.org/v1alpha1"
	"testing"
)

func TestEnabledSchemes(t *testing.T) {
	testES := EnabledSchemes{}

	if testES.String() != "" {
		t.Errorf("empty EnabledSchemes converted no-empty string %s", testES.String())
	}

	if !testES.Empty() {
		t.Error("Empty method returned false for empty EnabledSchemes")
	}

	if testES.Set(v1alpha1.HadoopClusterKind) != nil {
		t.Error("failed to restore TFJob")
	} else {
		stored := false
		for _, kind := range testES {
			if kind == v1alpha1.HadoopClusterKind {
				stored = true
			}
		}
		if !stored {
			t.Errorf("%s not successfully registered", v1alpha1.HadoopClusterKind)
		}
	}

	dummyJob := "dummyjob"
	if testES.Set(dummyJob) == nil {
		t.Errorf("successfully registered non-supported job %s", dummyJob)
	}

	if testES.Empty() {
		t.Error("Empty method returned true for non-empty EnabledSchemes")
	}

	es2 := EnabledSchemes{}
	es2.FillAll()
	if es2.Empty() {
		t.Error("Empty method returned true for fully registered EnabledSchemes")
	}
}
