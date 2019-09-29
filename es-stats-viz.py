import json
import squarify
import argparse
import svgwrite
import os.path
import re
import colour
import random

parser = argparse.ArgumentParser(description='visualise diagnostics')
parser.add_argument('diagnostics_dir', metavar='DIR', help='a path to an unpacked diagnostics dump')
args = parser.parse_args()

nodes = {}

with open(os.path.join(args.diagnostics_dir, 'nodes_stats.json')) as f:
  stats = json.load(f)
  for nodeId, nodeStats in stats['nodes'].items():
    node =  { 'name': nodeStats['name']
            , 'heap_max_in_bytes': nodeStats['jvm']['mem']['heap_max_in_bytes']
            , 'disk_bytes_by_path': {}
            }
    for pathStats in nodeStats['fs']['data']:
      node['disk_bytes_by_path'][pathStats['path']] = pathStats['total_in_bytes']
    nodes[nodeId] = node

def normalizeIndexName(indexName):
  indexName = re.sub('[0-9]{6,}', '', indexName)
  indexName = re.sub('[0-9]{4}\.[0-9]{2}\.[0-9]{2}', '', indexName)
  return indexName

random.seed(42)
colors = {}

with open(os.path.join(args.diagnostics_dir, 'indices_stats.json')) as f:
  stats = json.load(f)
  for indexName, indexStats in stats['indices'].items():
    normalizedIndexName = normalizeIndexName(indexName)
    if normalizedIndexName not in colors:
      newColor = colour.Color(hue=random.random(), saturation=1, luminance=0.8)
      colors[normalizedIndexName] = newColor.hex_l
    color = colors[normalizedIndexName]

    for shardNum, shardStats in indexStats['shards'].items():
      for shardCopy in shardStats:
        nodeId = shardCopy['routing']['node']
        primary = shardCopy['routing']['primary']
        storeSize = shardCopy['store']['size_in_bytes']
        translogSize = shardCopy['translog']['size_in_bytes']
        segmentMemory = shardCopy['segments']['memory_in_bytes']
        path = shardCopy['shard_path']['data_path']
        shardName = '[{}][{}]'.format(indexName, shardNum)

        if nodeId not in nodes:
          nodes[nodeId] = {}
        node = nodes[nodeId]

        if 'shards_by_path' not in node:
          node['shards_by_path'] = {}

        if path not in node['shards_by_path']:
          node['shards_by_path'][path] = {}
        pathContents = node['shards_by_path'][path]

        pathContents[shardName] = {
          'primary': primary,
          'store': storeSize,
          'translog': translogSize,
          'segment_memory': segmentMemory,
          'color': color
        }

def makeDiskSizeTree():
  rootNode = {'label': 'cluster', 'children': [], 'x': 0, 'y': 0, 'dx': 1000, 'dy': 800}
  for nodeId, nodeContents in nodes.items():
    if 'shards_by_path' in nodeContents:
      nodeNode = {'label': nodes[nodeId]['name'], 'children': []}
      rootNode['children'].append(nodeNode)
      for path, pathContents in nodeContents['shards_by_path'].items():
        pathNode = {'label': path, 'children': [], 'total': nodeContents['disk_bytes_by_path'][path]}
        nodeNode['children'].append(pathNode)
        for shardName, shardDetails in pathContents.items():
          shardComponents = []
          if shardDetails['store'] > 0:
            shardComponents.append({ 'label': 'store'
                                   , 'color': shardDetails['color']
                                   , 'total': shardDetails['store']
                                   })
          if shardDetails['translog'] > 0:
            shardComponents.append({ 'label': 'translog'
                                   , 'color': shardDetails['color']
                                   , 'total': shardDetails['translog']
                                   })
          pathNode['children'].append(
            { 'label': shardName
            , 'children': shardComponents
            })
  return rootNode

def makeSegmentMemoryTree():
  rootNode = {'label': 'cluster', 'children': [], 'x': 0, 'y': 0, 'dx': 1000, 'dy': 800}
  for nodeId, nodeContents in nodes.items():
    if 'shards_by_path' in nodeContents:
      nodeNode = {'label': nodes[nodeId]['name'], 'children': [], 'total': nodeContents['heap_max_in_bytes']}
      rootNode['children'].append(nodeNode)
      for path, pathContents in nodeContents['shards_by_path'].items():
        for shardName, shardDetails in pathContents.items():
          if shardDetails['segment_memory'] > 0:
            nodeNode['children'].append(
              { 'label': shardName
              , 'color': shardDetails['color']
              , 'total': shardDetails['segment_memory']
              })
  return rootNode

def calculateSizes(node):
  childrenSize = 0
  if 'children' in node:
    for child in node['children']:
      calculateSizes(child)
    for child in node['children']:
      childrenSize += child['size']
    node['childrenSize'] = childrenSize

  if 'total' in node:
    node['size'] = node['total']
  else:
    node['size'] = childrenSize

def calculatePositions(node, padding_threshold, level=0):
  if 'children' in node:
    x  = node['x']
    y  = node['y']
    dx = node['dx']
    dy = node['dy']

    if level == 0:

      node['children'].sort(key=lambda c: c['label'])

      padding = 5
      if padding * (len(node['children']) - 1) > dx:
        padding = 0

      dxMinusTotalPadding = dx - padding * (len(node['children']) - 1)
      childX = x
      for child in node['children']:
        child['y']  = y
        child['dy'] = dy
        child['x']  = childX
        child['dx'] = child['size'] * dxMinusTotalPadding / node['size']
        childX += child['dx'] + padding
        calculatePositions(child, padding_threshold, level+1)

    else:
      if 'childrenSize' in node:
        childrenRatio = node['childrenSize'] / node['size']
        if childrenRatio < 1:
          if dx > dy:
            dx = dx * childrenRatio
          else:
            dy = dy * childrenRatio

      node['children'].sort(reverse=True, key=lambda c: c['size'])
      sizes = squarify.normalize_sizes(list(map(lambda c: c['size'], node['children'])), dx, dy)
      if level < padding_threshold:
        rects = squarify.padded_squarify(sizes, x, y, dx, dy)
      else:
        rects = squarify.squarify(sizes, x, y, dx, dy)
      for child, rect in zip(node['children'], rects):
        for k, v in rect.items():
          child[k] = v
        calculatePositions(child, padding_threshold, level+1)

def renderSvg(rootNode, filename):
  d = svgwrite.Drawing(viewBox=("{} {} {} {}".format(rootNode['x'], rootNode['y'], rootNode['dx'], rootNode['dy'])))

  def renderTree(node, parentLabel=''):
    label = parentLabel + ' -> ' + node['label']
    if 'children' in node:
      if 'total' in node:
        rect = d.rect((node['x'], node['y']), (node['dx'], node['dy']), stroke='none', fill='gainsboro')
        rect.set_desc(title=label)
        d.add(rect)
      for child in node['children']:
        renderTree(child, label)
    else:
      rect = d.rect((node['x'], node['y']), (node['dx'], node['dy']), stroke='black', fill=node['color'])
      rect.set_desc(title=label)
      d.add(rect)

  renderTree(rootNode)
  d.saveas(filename)

diskSizeTree = makeDiskSizeTree()
calculateSizes(diskSizeTree)
calculatePositions(diskSizeTree, 2)
renderSvg(diskSizeTree, 'diskSize.svg')

segmentMemoryTree = makeSegmentMemoryTree()
calculateSizes(segmentMemoryTree)
calculatePositions(segmentMemoryTree, 1)
renderSvg(segmentMemoryTree, 'segmentMemory.svg')
