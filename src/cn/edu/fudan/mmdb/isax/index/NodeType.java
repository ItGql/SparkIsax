package cn.edu.fudan.mmdb.isax.index;

/**
 * Defines node types.
 * 
 * @author Josh Patterson
 * 
 */
public enum NodeType {
  ROOT,
  /** The inner node type. */
  INTERNAL,
  /** The leaf node type. */
  TERMINAL;
}
