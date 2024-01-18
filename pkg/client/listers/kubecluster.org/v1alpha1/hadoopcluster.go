/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	v1alpha1 "github.com/chriskery/hadoop-operator/pkg/apis/kubecluster.org/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// HadoopClusterLister helps list HadoopClusters.
// All objects returned here must be treated as read-only.
type HadoopClusterLister interface {
	// List lists all HadoopClusters in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.HadoopCluster, err error)
	// HadoopClusters returns an object that can list and get HadoopClusters.
	HadoopClusters(namespace string) HadoopClusterNamespaceLister
	HadoopClusterListerExpansion
}

// hadoopClusterLister implements the HadoopClusterLister interface.
type hadoopClusterLister struct {
	indexer cache.Indexer
}

// NewHadoopClusterLister returns a new HadoopClusterLister.
func NewHadoopClusterLister(indexer cache.Indexer) HadoopClusterLister {
	return &hadoopClusterLister{indexer: indexer}
}

// List lists all HadoopClusters in the indexer.
func (s *hadoopClusterLister) List(selector labels.Selector) (ret []*v1alpha1.HadoopCluster, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.HadoopCluster))
	})
	return ret, err
}

// HadoopClusters returns an object that can list and get HadoopClusters.
func (s *hadoopClusterLister) HadoopClusters(namespace string) HadoopClusterNamespaceLister {
	return hadoopClusterNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// HadoopClusterNamespaceLister helps list and get HadoopClusters.
// All objects returned here must be treated as read-only.
type HadoopClusterNamespaceLister interface {
	// List lists all HadoopClusters in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.HadoopCluster, err error)
	// Get retrieves the HadoopCluster from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.HadoopCluster, error)
	HadoopClusterNamespaceListerExpansion
}

// hadoopClusterNamespaceLister implements the HadoopClusterNamespaceLister
// interface.
type hadoopClusterNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all HadoopClusters in the indexer for a given namespace.
func (s hadoopClusterNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.HadoopCluster, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.HadoopCluster))
	})
	return ret, err
}

// Get retrieves the HadoopCluster from the indexer for a given namespace and name.
func (s hadoopClusterNamespaceLister) Get(name string) (*v1alpha1.HadoopCluster, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("hadoopcluster"), name)
	}
	return obj.(*v1alpha1.HadoopCluster), nil
}
