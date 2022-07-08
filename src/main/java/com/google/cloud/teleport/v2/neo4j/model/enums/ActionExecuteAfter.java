package com.google.cloud.teleport.v2.neo4j.model.enums;

/**
 * Allows creation of run-time dependencies.
 *
 * These are execution stages:
 *  start,
 *     sources,
 *     nodes,
 *     edges,
 *     loads,
 *     preloads
 *
 * These refer to specific items when qualified with a name:
 *
 *     source,
 *     node,
 *     edge,
 *
 *  Currently: async==start==preloads
 */
public enum ActionExecuteAfter {
    start,
    sources,
    nodes,
    edges,
    source,
    node,
    edge,
    async,
    loads,
    preloads

}
