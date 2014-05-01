#!/usr/bin/env python2
import rados
import rbd
import json

# helper to find commands:
# [x for x in json.loads(ceph_argparse.json_command(cluster, prefix="get_command_descriptions", argdict=dict(format='json'))[1]).values() if 'get' in x['sig'] and 'pool' in x['sig']]

def get_pool_size(cluster, poolname):
	query = {'format': 'json',
	         'module': 'osd',
	         'perm': 'r',
	         'pool': poolname,
	         'prefix': 'osd pool get',
	         'var': 'size'}
	results = cluster.mon_command(json.dumps(query), '')
	return json.loads(results[1])['size']

def get_cluster_space(cluster):
	query = {'format': 'json',
	         'module': 'mon',
	         'perm': 'r',
	         'prefix': 'df'}
	results = cluster.mon_command(json.dumps(query), '')
	return json.loads(results[1])
	

def get_provisioned_size(cluster):
	# Return the bytes allocated for each pool, with a special _TOTAL_ value
	pool_provisioned = {}
	clustersum = 0
	cluster_space = get_cluster_space(cluster)
	pools_storing = {pool['name']:pool['stats']['bytes_used'] for pool in cluster_space['pools']}
	for poolname in cluster.list_pools():
		poolsum = 0
		pool_size = get_pool_size(cluster, poolname)
		# pools_storing (ceph df) doesn't show replication size, just data size
		pools_storing[poolname] = pools_storing[poolname] * pool_size
		with cluster.open_ioctx(poolname) as ioctx:
			rbd_inst = rbd.RBD()
			for rbdname in rbd_inst.list(ioctx):
				with rbd.Image(ioctx, rbdname, read_only=True) as image:
					image_size = image.size()
					real_image_size = image_size * pool_size
					poolsum += real_image_size
					#print "  %s %iM"%(rbdname, image.size()/1024/1024)
		#print("%-20s %10iM/%iM"%(poolname, pools_storing[poolname]/1024/1024, poolsum/1024/1024))
		pool_provisioned[poolname] = poolsum
		clustersum += poolsum
	pool_provisioned['_TOTAL_'] = clustersum
	return pool_provisioned
#print("%-20s %10iM/%iM out of %iM"%('Total', cluster_space['stats']['total_used']/1024, clustersum/1024/1024, cluster_space['stats']['total_space']/1024))
