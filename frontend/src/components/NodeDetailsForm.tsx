import type { EditorCatalog, GraphDefinition, GraphNode } from "../lib/types";

type NodeDetailsFormProps = {
  graph: GraphDefinition | null;
  node: GraphNode;
  catalog?: EditorCatalog | null;
  onNodeChange: (node: GraphNode) => void;
};

export function NodeDetailsForm({
  graph,
  node,
  catalog = null,
  onNodeChange,
}: NodeDetailsFormProps) {
  const contract = catalog?.contracts[node.category];

  return (
    <div className="modal-folder-section">
      <label>
        Node ID
        <input value={node.id} readOnly />
      </label>
      <label>
        Label
        <input
          value={node.label}
          onChange={(event) => onNodeChange({ ...node, label: event.target.value })}
        />
      </label>
      <label>
        Description
        <textarea
          rows={3}
          value={node.description ?? ""}
          onChange={(event) => onNodeChange({ ...node, description: event.target.value })}
        />
      </label>
      <label>
        Position X
        <input
          type="number"
          value={node.position.x}
          onChange={(event) => onNodeChange({ ...node, position: { ...node.position, x: Number(event.target.value) } })}
        />
      </label>
      <label>
        Position Y
        <input
          type="number"
          value={node.position.y}
          onChange={(event) => onNodeChange({ ...node, position: { ...node.position, y: Number(event.target.value) } })}
        />
      </label>
      <div className="inspector-meta">
        <span>Category: {node.category}</span>
        <span>Kind: {node.kind}</span>
        <span>Provider: {node.provider_label}</span>
      </div>
      {contract ? (
        <div className="contract-card">
          <strong>Contract</strong>
          <span>Accepts: {contract.accepted_inputs.join(", ")}</span>
          <span>Produces: {contract.produced_outputs.join(", ")}</span>
        </div>
      ) : null}
    </div>
  );
}
